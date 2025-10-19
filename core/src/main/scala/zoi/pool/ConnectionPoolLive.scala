package zoi.pool

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

import zio.{Clock, Duration, Exit, IO, Promise, Random, Ref, Runtime, Scope, UIO, Unsafe, ZIO, durationInt}

private[pool] final class ConnectionPoolLive(
  config: PoolConfig,
  hooks: PoolHooks,
  factory: ConnectionFactory,
  core: HandoffCore[PooledConnection],
  registry: Ref[Set[PooledConnection]],
  suspendGate: Ref[Option[Promise[Nothing, Unit]]],
  runtime: Runtime[Any],
) extends ConnectionPool {

  private val connectionTimeoutNanos = config.connectionTimeout.toNanos
  private val bypassWindowNanos      = config.aliveBypassWindow.toNanos
  private val leakThresholdNanos     = config.leakDetectionThreshold.toNanos
  private val idleTimeoutNanos       = config.idleTimeout.toNanos
  private val keepaliveNanos         = config.keepaliveTime.toNanos
  private val recorder               = hooks.metrics
  private val metricsEnabled         = recorder ne PoolMetrics.none

  private val releaseFromJdbc: ConnectionHandle => Unit =
    handle =>
      Unsafe.unsafe { implicit unsafe =>
        runtime.unsafe.run(returnHandle(handle)).getOrThrowFiberFailure()
      }

  val dataSource: DataSource = new PoolDataSource(this, config)

  def connection: ZIO[Scope, SQLException, Connection] =
    ZIO.acquireReleaseExit(checkoutHandle)(releaseHandle)

  private[pool] def checkoutHandle: IO[SQLException, ConnectionHandle] =
    if (metricsEnabled) timedCheckout else plainCheckout

  private def plainCheckout: IO[SQLException, ConnectionHandle] =
    awaitResume *> Clock.nanoTime.flatMap(now =>
      acquireLoop(now + connectionTimeoutNanos, now).map(acquired => handleFor(acquired.pooled)),
    )

  private def timedCheckout: IO[SQLException, ConnectionHandle] =
    awaitResume *> Clock.nanoTime.flatMap { start =>
      acquireLoop(start + connectionTimeoutNanos, start).foldZIO(
        failure => ZIO.succeed(noteAcquireFailure(failure)) *> ZIO.fail(failure),
        acquisition =>
          Clock.nanoTime.map { end =>
            recorder.acquireSucceeded(end - start, acquisition.waited)
            handleFor(acquisition.pooled)
          },
      )
    }

  private def handleFor(pooled: PooledConnection): ConnectionHandle =
    new ConnectionHandle(pooled, releaseFromJdbc, markBroken(pooled))

  private def noteAcquireFailure(failure: SQLException): Unit =
    failure match {
      case _: PoolTimeoutException => recorder.acquireTimedOut()
      case _                       => ()
    }

  private def awaitResume: UIO[Unit] =
    suspendGate.get.flatMap {
      case None       => ZIO.unit
      case Some(gate) => gate.await
    }

  /**
   * The uncontended path is one clock read, one hand-off and one pure check:
   * a connection returned moments ago is known good, so nothing is validated
   * and nothing is scheduled on the blocking executor.
   */
  private def acquireLoop(
    deadlineNanos: Long,
    nowNanos: Long,
  ): IO[SQLException, ConnectionPoolLive.Acquisition] = {
    val remaining = deadlineNanos - nowNanos
    if (remaining <= 0L)
      ZIO.fail(new PoolTimeoutException(config.poolName, config.connectionTimeout))
    else
      core.acquire(Duration.fromNanos(remaining)).flatMap {
        case HandoffCore.Acquired.Reserved(waited)      =>
          createConnection
            .onInterrupt(core.releaseSlot)
            .foldZIO(
              failure => core.releaseSlot *> retryCreate(deadlineNanos, failure),
              pooled => borrowed(pooled, waited),
            )
        case HandoffCore.Acquired.Ready(pooled, waited) =>
          if (!waited && knownGood(pooled, nowNanos)) borrowed(pooled, waited)
          else recheck(pooled, waited, deadlineNanos)
      }
  }

  private def knownGood(pooled: PooledConnection, nowNanos: Long): Boolean =
    !pooled.broken &&
      !pooled.expiredAt(nowNanos) &&
      nowNanos - pooled.lastReturnedNanos <= bypassWindowNanos

  private def recheck(
    pooled: PooledConnection,
    waited: Boolean,
    deadlineNanos: Long,
  ): IO[SQLException, ConnectionPoolLive.Acquisition] =
    Clock.nanoTime.flatMap { now =>
      if (knownGood(pooled, now)) borrowed(pooled, waited)
      else if (pooled.broken || pooled.expiredAt(now))
        destroy(pooled) *> acquireLoop(deadlineNanos, now)
      else
        factory.validate(pooled.raw).flatMap {
          case true  =>
            pooled.lastValidatedNanos = now
            pooled.lastReturnedNanos = now
            borrowed(pooled, waited)
          case false => destroy(pooled) *> acquireLoop(deadlineNanos, now)
        }
    }

  private def borrowed(
    pooled: PooledConnection,
    waited: Boolean,
  ): UIO[ConnectionPoolLive.Acquisition] =
    if (!config.leakDetectionEnabled)
      ZIO.succeed(ConnectionPoolLive.Acquisition(pooled, waited))
    else
      Clock.nanoTime.map { now =>
        pooled.leakReported = false
        pooled.borrowedAtNanos = now
        pooled.borrowed = true
        ConnectionPoolLive.Acquisition(pooled, waited)
      }

  /** A database that is briefly unreachable is retried inside the caller's budget. */
  private def retryCreate(
    deadlineNanos: Long,
    failure: SQLException,
  ): IO[SQLException, ConnectionPoolLive.Acquisition] =
    Clock.nanoTime.flatMap { now =>
      val remaining = deadlineNanos - now
      if (remaining <= 0L) ZIO.fail(failure)
      else
        ZIO.sleep(Duration.fromNanos(math.min(remaining, ConnectionPoolLive.RetryDelayNanos))) *>
          acquireLoop(deadlineNanos, now)
    }

  def state: UIO[PoolState] =
    for {
      idle      <- core.idleCount
      total     <- core.totalCount
      waiting   <- core.waitingCount
      shutdown  <- core.isShutdown
      suspended <- suspendGate.get.map(_.isDefined)
    } yield PoolState(
      active = math.max(total - idle, 0),
      idle = idle,
      total = total,
      waiting = waiting,
      suspended = suspended,
      shutdown = shutdown,
    )

  def metrics: UIO[PoolMetricsSnapshot] = state.map(recorder.counters.withState)

  /** Drops a connection the caller knows is bad; it is closed when returned. */
  def invalidate(connection: Connection): UIO[Unit] =
    ZIO.succeed {
      connection match {
        case handle: ConnectionHandle if handle.active => handle.pooled.broken = true
        case _                                         => ()
      }
    }

  /** Stops handing out connections; borrowers already holding one keep it. */
  def suspend: UIO[Unit] =
    Promise.make[Nothing, Unit].flatMap { fresh =>
      suspendGate.update {
        case None     => Some(fresh)
        case existing => existing
      }
    }

  def resume: UIO[Unit] =
    suspendGate.getAndSet(None).flatMap {
      case Some(gate) => gate.succeed(()).unit
      case None       => ZIO.unit
    }

  /** Borrows from a synchronous caller, translating failure into JDBC's terms. */
  private[pool] def borrowUnsafe(): Connection =
    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe.run(checkoutHandle.either).getOrThrowFiberFailure() match {
        case Right(handle) => handle
        case Left(failure) => throw failure
      }
    }

  /**
   * A borrower's own failure is the cheapest health signal there is: a fatal
   * SQLException means the connection is not pooled again, and nothing had to
   * be proxied or round-tripped to learn it.
   */
  private def markBroken(pooled: PooledConnection): SQLException => Unit =
    failure =>
      if (config.failureTracking && hooks.classification(failure) == SqlExceptionClassification.Fatal)
        pooled.broken = true

  private def releaseHandle(handle: ConnectionHandle, exit: Exit[Any, Any]): UIO[Unit] =
    ZIO.suspendSucceed {
      if (config.failureTracking) noteExit(handle, exit)
      returnHandle(handle)
    }

  private def noteExit(handle: ConnectionHandle, exit: Exit[Any, Any]): Unit =
    exit match {
      case Exit.Failure(cause) =>
        val raised = cause.failures.collect { case failure: Throwable => failure } ++ cause.defects
        if (raised.exists(hooks.classification(_) == SqlExceptionClassification.Fatal))
          handle.pooled.broken = true
      case _                   => ()
    }

  private[pool] def returnHandle(handle: ConnectionHandle): UIO[Unit] =
    ZIO.suspendSucceed {
      if (!handle.claimRelease()) ZIO.unit
      else {
        handle.closeTrackedStatements()
        checkin(handle.pooled)
      }
    }

  private[pool] def createConnection: IO[SQLException, PooledConnection] =
    for {
      raw      <- factory.open
      _        <- factory.configure(raw).tapError(_ => factory.close(raw))
      _        <- verifyNew(raw)
      now      <- Clock.nanoTime
      lifetime <- lifetimeNanos
      cache     = Option.when(config.statementCacheEnabled)(
                    new StatementCache(raw, config.statementCacheSize),
                  )
      pooled    = new PooledConnection(raw, now, lifetime, cache)
      _        <- ZIO.succeed(recorder.connectionCreated())
      _        <- registry.update(_ + pooled)
    } yield pooled

  /** A new connection is proved usable once, so bad setup fails at the source. */
  private def verifyNew(raw: Connection): IO[SQLException, Unit] =
    factory.validate(raw).flatMap {
      case true  => ZIO.unit
      case false =>
        factory.close(raw) *> ZIO.fail(
          new ConnectionCreationException(
            config.poolName,
            new SQLException("a new connection did not pass validation"),
          ),
        )
    }

  /** Spreads retirement so a pool does not replace every connection at once. */
  private def lifetimeNanos: UIO[Long] = {
    val base = config.maxLifetime.toNanos
    val span = base / 40L
    if (!config.maxLifetimeEnabled || span <= 0L) ZIO.succeed(base)
    else Random.nextLongBetween(base - span, base + 1L)
  }

  private[pool] def checkin(pooled: PooledConnection): UIO[Unit] =
    Clock.nanoTime.flatMap { now =>
      pooled.borrowed = false
      if (pooled.broken || pooled.expiredAt(now)) destroy(pooled)
      else {
        val wasDirty = pooled.stateDirty
        pooled.stateDirty = false
        factory.reset(pooled.raw, wasDirty).flatMap {
          case false => destroy(pooled)
          case true  =>
            pooled.lastReturnedNanos = now
            core.offer(pooled).flatMap {
              case HandoffCore.Offered.Pooled    => ZIO.unit
              case HandoffCore.Offered.Discarded => destroy(pooled)
            }
        }
      }
    }

  private[pool] def destroy(pooled: PooledConnection): UIO[Unit] =
    registry.update(_ - pooled) *>
      ZIO.attemptBlocking(pooled.close()).ignore *>
      core.releaseSlot <* ZIO.succeed(recorder.connectionClosed())

  private[pool] def maintenanceLoop: UIO[Unit] =
    (ZIO.sleep(config.effectiveMaintenanceInterval) *> maintain).forever

  private[pool] def maintain: UIO[Unit] =
    Clock.nanoTime.flatMap { now =>
      retireExpired(now) *>
        retireIdle(now) *>
        keepalive(now) *>
        reportLeaks(now) *>
        prefill(config.effectiveMinimumIdle)
    }

  private def retireExpired(now: Long): UIO[Unit] =
    if (!config.maxLifetimeEnabled) ZIO.unit
    else
      core
        .takeIdleWhere(Int.MaxValue, _.expiredAt(now))
        .flatMap(ZIO.foreachDiscard(_)(retire))

  private def retireIdle(now: Long): UIO[Unit] =
    if (!config.idleTimeoutEnabled) ZIO.unit
    else
      core.totalCount.flatMap { total =>
        core
          .takeIdleWhere(
            total - config.effectiveMinimumIdle,
            pooled => pooled.idleSince(now) >= idleTimeoutNanos,
          )
          .flatMap(ZIO.foreachDiscard(_)(retire))
      }

  private def retire(pooled: PooledConnection): UIO[Unit] =
    ZIO.succeed(recorder.connectionRetired()) *> destroy(pooled)

  private def keepalive(now: Long): UIO[Unit] =
    if (!config.keepaliveEnabled) ZIO.unit
    else
      core
        .takeIdleWhere(Int.MaxValue, now - _.lastValidatedNanos >= keepaliveNanos)
        .flatMap(ZIO.foreachDiscard(_)(probe(now)))

  private def probe(now: Long)(pooled: PooledConnection): UIO[Unit] =
    factory.validate(pooled.raw).flatMap {
      case false => destroy(pooled)
      case true  =>
        ZIO.succeed(pooled.lastValidatedNanos = now) *> core.offer(pooled).flatMap {
          case HandoffCore.Offered.Pooled    => ZIO.unit
          case HandoffCore.Offered.Discarded => destroy(pooled)
        }
    }

  private def reportLeaks(now: Long): UIO[Unit] =
    if (!config.leakDetectionEnabled) ZIO.unit
    else
      registry.get.flatMap { all =>
        ZIO.foreachDiscard(all.filter(leaking(now)))(report(now))
      }

  private def leaking(now: Long)(pooled: PooledConnection): Boolean =
    pooled.borrowed &&
      !pooled.leakReported &&
      now - pooled.borrowedAtNanos >= leakThresholdNanos

  private def report(now: Long)(pooled: PooledConnection): UIO[Unit] = {
    val heldMillis = (now - pooled.borrowedAtNanos) / 1000000L
    ZIO.succeed { pooled.leakReported = true; recorder.leakSuspected() } *>
      ZIO.logWarning(
        s"${config.poolName} - connection ${java.lang.System.identityHashCode(pooled)} " +
          s"has been held for ${heldMillis}ms, which may be a leak",
      )
  }

  /** Opens connections until the pool holds `target` of them. */
  private[pool] def prefill(target: Int): UIO[Unit] =
    core.totalCount.flatMap { total =>
      ZIO.foreachDiscard(1 to (target - total))(_ => fillOne).when(target > total).unit
    }

  private def fillOne: UIO[Unit] =
    core.tryReserve.flatMap {
      case false => ZIO.unit
      case true  =>
        createConnection.foldZIO(
          _ => core.releaseSlot,
          pooled =>
            core.offer(pooled).flatMap {
              case HandoffCore.Offered.Pooled    => ZIO.unit
              case HandoffCore.Offered.Discarded => destroy(pooled)
            },
        )
    }

  private[pool] def shutdown: UIO[Unit] =
    resume *>
      core.shutdown *>
      core.drainIdle.flatMap(ZIO.foreachDiscard(_)(destroy)) *>
      awaitQuiet *>
      abortRemaining

  private def awaitQuiet: UIO[Unit] =
    core.totalCount
      .flatMap {
        case remaining if remaining <= 0 => ZIO.unit
        case _                           => ZIO.sleep(5.millis) *> awaitQuiet
      }
      .interruptible
      .timeout(config.shutdownTimeout)
      .unit

  private def abortRemaining: UIO[Unit] =
    registry.getAndSet(Set.empty).flatMap(ZIO.foreachDiscard(_)(pooled => factory.abort(pooled.raw)))
}

private[pool] object ConnectionPoolLive {

  private[pool] final case class Acquisition(pooled: PooledConnection, waited: Boolean)

  private val RetryDelayNanos: Long = 50L * 1000000L

  def scoped(config: PoolConfig, hooks: PoolHooks): ZIO[Scope, SQLException, ConnectionPoolLive] =
    for {
      factory  <- ConnectionFactory.make(config)
      core     <- HandoffCore.make[PooledConnection](config.poolName, config.maximumPoolSize)
      registry <- Ref.make(Set.empty[PooledConnection])
      gate     <- Ref.make(Option.empty[Promise[Nothing, Unit]])
      runtime  <- ZIO.runtime[Any]
      pool      = new ConnectionPoolLive(config, hooks, factory, core, registry, gate, runtime)
      _        <- ZIO.addFinalizer(pool.shutdown)
      _        <- pool.prefill(config.initialSize)
      _        <- hooks.metrics.install(pool.metrics)
      _        <- PoolManagement.registered(pool, config, runtime)
      _        <- pool.maintenanceLoop.forkScoped
    } yield pool
}
