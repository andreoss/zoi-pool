package zoi.pool.examples

import zio.{Console, ZIO, ZIOAppDefault}

import zoi.pool.{ConnectionPool, PoolConfig, PoolHooks, PoolMetrics}

/** Run a pool with metrics and a management bean switched on. */
object Observed extends ZIOAppDefault {

  private val config = PoolConfig(
    url = "jdbc:h2:mem:observed;DB_CLOSE_DELAY=-1",
    poolName = "orders",
    maximumPoolSize = 4,
    jmxEnabled = true,
  )

  private val hooks = PoolHooks(metrics = PoolMetrics.zio("orders"))

  override def run =
    ZIO.scoped {
      for {
        pool    <- ConnectionPool.scoped(config, hooks)
        _       <- ZIO.foreachParDiscard(1 to 20)(_ => ZIO.scoped(pool.connection))
        metrics <- pool.metrics
        _       <- Console.printLine(
          s"acquires=${metrics.acquires} created=${metrics.connectionsCreated} " +
            s"mean acquire=${metrics.averageAcquireNanos.toLong}ns",
        )
      } yield ()
    }
}
