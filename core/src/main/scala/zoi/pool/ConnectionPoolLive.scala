package zoi.pool

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

import zio.{Clock, Duration, Exit, IO, Promise, Random, Ref, Runtime, Scope, UIO, Unsafe, ZIO, durationInt}

private[pool] final class ConnectionPoolLive(
  config: PoolConfig,
  hooks: PoolHooks,
  factory: ConnectionFactory,
  core: PoolCore[PooledConnection],
  registry: Ref[Set[PooledConnection]],
  suspendGate: Ref[Option[Promise[Nothing, Unit]]],
  runtime: Runtime[Any],
) extends ConnectionPool {

  private val connectionTimeoutNanos = config.connectionTimeout.toNanos
  private val bypassWindowNanos      = config.aliveBypassWindow.toNanos
  private val leakThresholdNanos     = config.leakDetectionThreshold.toNanos
  private val idleTimeoutNanos       = config.idleTimeout.toNanos
  private val keepaliveNanos         = config.keepaliveTime.toNanos

  private val releaseFromJdbc: ConnectionHandle => Unit =
    handle =>
      Unsafe.unsafe { implicit unsafe =>
        runtime.unsafe.run(returnHandle(handle)).getOrThrowFiberFailure()
      }

  val dataSource: DataSource = new PoolDataSource(this, config)

  def connection: ZIO[Scope, SQLException, Connection] =
    ZIO
      .acquireReleaseExit(checkoutHandle) { (handle, exit) =>
        noteExit(handle, exit) *> returnHandle(handle)
      }
      .map(handle => handle: Connection)

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

  private[pool] def checkoutHandle: IO[SQLException, ConnectionHandle] =
    checkout.map { pooled =>
      new ConnectionHandle(pooled, releaseFromJdbc, markBroken(pooled))
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

  private def noteExit(handle: ConnectionHandle, exit: Exit[Any, Any]): UIO[Unit] =
    ZIO.succeed {
      if (config.failureTracking) exit match {
        case Exit.Failure(cause) =>
          val raised = cause.failures.collect { case failure: Throwable => failure } ++ cause.defects
          if (raised.exists(hooks.classification(_) == SqlExceptionClassification.Fatal))
            handle.pooled.broken = true
        case _                   => ()
      }
    }

  private[pool] def returnHandle(handle: ConnectionHandle): UIO[Unit] =
    ZIO.succeed(handle.claimRelease()).flatMap {
      case false => ZIO.unit
      case true  => ZIO.succeed(handle.closeTrackedStatements()) *> checkin(handle.pooled)
    }

  private[pool] def checkout: IO[SQLException, PooledConnection] =
    awaitResume *> Clock.nanoTime.flatMap(now => acquireLoop(now + connectionTimeoutNanos))

  private def awaitResume: UIO[Unit] =
    suspendGate.get.flatMap {
      case None       => ZIO.unit
      case Some(gate) => gate.await
    }

  private def acquireLoop(deadlineNanos: Long): IO[SQLException, PooledConnection] =
    budget(deadlineNanos).flatMap { remaining =>
      core.acquire(remaining).flatMap {
        case PoolCore.Acquired.Reserved         =>
          createConnection
            .onInterrupt(core.releaseSlot)
            .foldZIO(
              failure => core.releaseSlot *> retryCreate(deadlineNanos, failure),
              markBorrowed,
            )
        case PoolCore.Acquired.Ready(pooled, _) =>
          usable(pooled).flatMap {
            case true  => markBorrowed(pooled)
            case false => destroy(pooled) *> acquireLoop(deadlineNanos)
          }
      }
    }

  /** A database that is briefly unreachable is retried inside the caller's budget. */
  private def retryCreate(
    deadlineNanos: Long,
    failure: SQLException,
  ): IO[SQLException, PooledConnection] =
    Clock.nanoTime.flatMap { now =>
      val remaining = deadlineNanos - now
      if (remaining <= 0L) ZIO.fail(failure)
      else
        ZIO.sleep(Duration.fromNanos(math.min(remaining, ConnectionPoolLive.RetryDelayNanos))) *>
          acquireLoop(deadlineNanos)
    }

  private def budget(deadlineNanos: Long): IO[SQLException, Duration] =
    Clock.nanoTime.flatMap { now =>
      val remaining = deadlineNanos - now
      if (remaining <= 0L)
        ZIO.fail(new PoolTimeoutException(config.poolName, config.connectionTimeout))
      else ZIO.succeed(Duration.fromNanos(remaining))
    }

  /** A pooled connection is usable if it is neither broken, retired nor dead. */
  private def usable(pooled: PooledConnection): UIO[Boolean] =
    Clock.nanoTime.flatMap { now =>
      if (pooled.broken || pooled.expiredAt(now)) ZIO.succeed(false)
      else if (now - pooled.lastReturnedNanos <= bypassWindowNanos) ZIO.succeed(true)
      else
        factory.validate(pooled.raw).tap { alive =>
          ZIO.succeed(pooled.lastValidatedNanos = now).when(alive)
        }
    }

  private def markBorrowed(pooled: PooledConnection): UIO[PooledConnection] =
    if (!config.leakDetectionEnabled) ZIO.succeed(pooled)
    else
      Clock.nanoTime.map { now =>
        pooled.leakReported = false
        pooled.borrowedAtNanos = now
        pooled.borrowed = true
        pooled
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
      ZIO.succeed(pooled.borrowed = false) *> {
        if (pooled.broken || pooled.expiredAt(now)) destroy(pooled)
        else
          resetState(pooled).flatMap {
            case false => destroy(pooled)
            case true  =>
              ZIO.succeed(pooled.lastReturnedNanos = now) *> core.offer(pooled).flatMap {
                case PoolCore.Offered.Pooled    => ZIO.unit
                case PoolCore.Offered.Discarded => destroy(pooled)
              }
          }
      }
    }

  private def resetState(pooled: PooledConnection): UIO[Boolean] =
    factory.reset(pooled.raw, pooled.stateDirty) <* ZIO.succeed(pooled.stateDirty = false)

  private[pool] def destroy(pooled: PooledConnection): UIO[Unit] =
    registry.update(_ - pooled) *>
      ZIO.attemptBlocking(pooled.close()).ignore *>
      core.releaseSlot

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
        .flatMap(ZIO.foreachDiscard(_)(destroy))

  private def retireIdle(now: Long): UIO[Unit] =
    if (!config.idleTimeoutEnabled) ZIO.unit
    else
      core.totalCount.flatMap { total =>
        core
          .takeIdleWhere(
            total - config.effectiveMinimumIdle,
            pooled => pooled.idleSince(now) >= idleTimeoutNanos,
          )
          .flatMap(ZIO.foreachDiscard(_)(destroy))
      }

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
          case PoolCore.Offered.Pooled    => ZIO.unit
          case PoolCore.Offered.Discarded => destroy(pooled)
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
    ZIO.succeed(pooled.leakReported = true) *>
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
              case PoolCore.Offered.Pooled    => ZIO.unit
              case PoolCore.Offered.Discarded => destroy(pooled)
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

  private val RetryDelayNanos: Long = 50L * 1000000L

  def scoped(config: PoolConfig, hooks: PoolHooks): ZIO[Scope, SQLException, ConnectionPoolLive] =
    for {
      factory  <- ConnectionFactory.make(config)
      core     <- PoolCore.make[PooledConnection](config.poolName, config.maximumPoolSize)
      registry <- Ref.make(Set.empty[PooledConnection])
      gate     <- Ref.make(Option.empty[Promise[Nothing, Unit]])
      runtime  <- ZIO.runtime[Any]
      pool      = new ConnectionPoolLive(config, hooks, factory, core, registry, gate, runtime)
      _        <- ZIO.addFinalizer(pool.shutdown)
      _        <- pool.prefill(config.initialSize)
      _        <- pool.maintenanceLoop.forkScoped
    } yield pool
}
