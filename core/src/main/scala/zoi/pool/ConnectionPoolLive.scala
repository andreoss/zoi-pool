package zoi.pool

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

import zio.{IO, Runtime, Scope, UIO, Unsafe, ZIO}

private[pool] final class ConnectionPoolLive(
  config: PoolConfig,
  factory: ConnectionFactory,
  core: PoolCore[PooledConnection],
  runtime: Runtime[Any],
) extends ConnectionPool {

  private val releaseFromJdbc: ConnectionHandle => Unit =
    handle =>
      Unsafe.unsafe { implicit unsafe =>
        runtime.unsafe.run(returnHandle(handle)).getOrThrowFiberFailure()
      }

  val dataSource: DataSource = new PoolDataSource(this, config)

  def connection: ZIO[Scope, SQLException, Connection] =
    ZIO.acquireRelease(checkoutHandle)(returnHandle).map(handle => handle: Connection)

  def state: UIO[PoolState] =
    for {
      idle     <- core.idleCount
      total    <- core.totalCount
      waiting  <- core.waitingCount
      shutdown <- core.isShutdown
    } yield PoolState(
      active = math.max(total - idle, 0),
      idle = idle,
      total = total,
      waiting = waiting,
      suspended = false,
      shutdown = shutdown,
    )

  /** Borrows from a synchronous caller, translating failure into JDBC's terms. */
  private[pool] def borrowUnsafe(): Connection =
    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe.run(checkoutHandle.either).getOrThrowFiberFailure() match {
        case Right(handle) => handle
        case Left(failure) => throw failure
      }
    }

  private[pool] def checkoutHandle: IO[SQLException, ConnectionHandle] =
    checkout.map(pooled => new ConnectionHandle(pooled, releaseFromJdbc))

  private[pool] def returnHandle(handle: ConnectionHandle): UIO[Unit] =
    ZIO.succeed(handle.claimRelease()).flatMap {
      case false => ZIO.unit
      case true  => ZIO.succeed(handle.closeTrackedStatements()) *> checkin(handle.pooled)
    }

  private[pool] def checkout: IO[SQLException, PooledConnection] =
    core.acquire(config.connectionTimeout).flatMap {
      case PoolCore.Acquired.Ready(pooled, _) => ZIO.succeed(pooled)
      case PoolCore.Acquired.Reserved         => create.onError(_ => core.releaseSlot)
    }

  private[pool] def checkin(pooled: PooledConnection): UIO[Unit] =
    if (pooled.broken) destroy(pooled)
    else
      core.offer(pooled).flatMap {
        case PoolCore.Offered.Pooled    => markReturned(pooled)
        case PoolCore.Offered.Discarded => destroy(pooled)
      }

  private def create: IO[SQLException, PooledConnection] =
    for {
      raw <- factory.open
      now <- ZIO.succeed(java.lang.System.nanoTime())
    } yield new PooledConnection(raw, now)

  private def markReturned(pooled: PooledConnection): UIO[Unit] =
    ZIO.succeed { pooled.lastReturnedNanos = java.lang.System.nanoTime() }

  private[pool] def destroy(pooled: PooledConnection): UIO[Unit] =
    factory.close(pooled.raw) *> core.releaseSlot

  private[pool] def shutdown: UIO[Unit] =
    core.shutdown *> core.drainIdle.flatMap(ZIO.foreachDiscard(_)(destroy))
}
