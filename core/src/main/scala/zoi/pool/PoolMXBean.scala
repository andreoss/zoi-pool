package zoi.pool

import java.lang.management.ManagementFactory
import javax.management.ObjectName

import zio.{Runtime, Scope, UIO, Unsafe, ZIO}

/**
 * The pool as a management bean, so an operator sees the same numbers a ZIO
 * caller sees and can pause a pool without a deployment.
 */
trait PoolMXBean {
  def getPoolName: String
  def getMaximumPoolSize: Int
  def getMinimumIdle: Int
  def getActiveConnections: Int
  def getIdleConnections: Int
  def getTotalConnections: Int
  def getBorrowersWaiting: Int
  def getConnectionsCreated: Long
  def getConnectionsClosed: Long
  def getConnectionsRetired: Long
  def getAcquires: Long
  def getAcquireTimeouts: Long
  def getLeaksSuspected: Long
  def getMeanAcquireMillis: Double
  def isSuspended: Boolean
  def isShutdown: Boolean
  def suspendPool(): Unit
  def resumePool(): Unit
}

final private[pool] class PoolManagement(
    pool: ConnectionPool,
    config: PoolConfig,
    runtime: Runtime[Any],
) extends PoolMXBean {

  private def now: PoolMetricsSnapshot = run(pool.metrics)
  private def poolState: PoolState     = run(pool.state)

  def getPoolName: String          = config.poolName
  def getMaximumPoolSize: Int      = config.maximumPoolSize
  def getMinimumIdle: Int          = config.effectiveMinimumIdle
  def getActiveConnections: Int    = poolState.active
  def getIdleConnections: Int      = poolState.idle
  def getTotalConnections: Int     = poolState.total
  def getBorrowersWaiting: Int     = poolState.waiting
  def getConnectionsCreated: Long  = now.connectionsCreated
  def getConnectionsClosed: Long   = now.connectionsClosed
  def getConnectionsRetired: Long  = now.connectionsRetired
  def getAcquires: Long            = now.acquires
  def getAcquireTimeouts: Long     = now.acquireTimeouts
  def getLeaksSuspected: Long      = now.leaksSuspected
  def getMeanAcquireMillis: Double = now.averageAcquireNanos / 1000000.0
  def isSuspended: Boolean         = poolState.suspended
  def isShutdown: Boolean          = poolState.shutdown
  def suspendPool(): Unit          = run(pool.suspend)
  def resumePool(): Unit           = run(pool.resume)

  private def run[A](effect: UIO[A]): A =
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(effect).getOrThrowFiberFailure())
}

private[pool] object PoolManagement {

  /** Registers the bean for the pool's lifetime; never fails the pool. */
  def registered(
      pool: ConnectionPool,
      config: PoolConfig,
      runtime: Runtime[Any],
  ): ZIO[Scope, Nothing, Unit] =
    if (!config.jmxEnabled) ZIO.unit
    else
      ZIO
        .acquireRelease(register(pool, config, runtime))(unregister)
        .unit

  private def register(
      pool: ConnectionPool,
      config: PoolConfig,
      runtime: Runtime[Any],
  ): UIO[Option[ObjectName]] =
    ZIO
      .attempt {
        val name   = objectName(config)
        val server = ManagementFactory.getPlatformMBeanServer
        if (!server.isRegistered(name))
          server.registerMBean(new PoolManagement(pool, config, runtime), name)
        Option(name)
      }
      .catchAll(_ => ZIO.succeed(None))

  private def unregister(name: Option[ObjectName]): UIO[Unit] =
    ZIO
      .attempt(name.foreach(ManagementFactory.getPlatformMBeanServer.unregisterMBean))
      .ignore

  private[pool] def objectName(config: PoolConfig): ObjectName =
    new ObjectName(s"${config.jmxDomain}:type=Pool,name=${ObjectName.quote(config.poolName)}")
}
