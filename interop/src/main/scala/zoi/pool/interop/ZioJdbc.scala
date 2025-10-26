package zoi.pool.interop

import zio.jdbc.{ZConnection, ZConnectionPool}
import zio.{UIO, ZEnvironment, ZLayer}
import zoi.pool.{ConnectionPool, PoolConfig, PoolHooks}

/**
 * zio-jdbc asks for a `ZConnectionPool` rather than a `DataSource`, so this is
 * the one consumer that needs an adapter. The pool underneath is zoi-pool's,
 * not another pool layered on top of it.
 */
object ZioJdbc {

  /** Presents an existing pool as the pool zio-jdbc expects. */
  def asZConnectionPool(pool: ConnectionPool): ZConnectionPool =
    new ZConnectionPool {
      def transaction: ZLayer[Any, Throwable, ZConnection] =
        ZLayer.scoped(pool.connection.flatMap(ZConnection.make))

      def invalidate(connection: ZConnection): UIO[Any] =
        connection.access(borrowed => borrowed).orDie.flatMap(pool.invalidate)
    }

  /** Builds a pool and hands zio-jdbc its view of it. */
  def layer(
    config: PoolConfig,
    hooks: PoolHooks = PoolHooks.default,
  ): ZLayer[Any, Throwable, ZConnectionPool] =
    ZLayer.scoped(ConnectionPool.scoped(config, hooks).map(asZConnectionPool))

  /** Both views of the same pool, for an application that uses each in places. */
  def layers(
    config: PoolConfig,
    hooks: PoolHooks = PoolHooks.default,
  ): ZLayer[Any, Throwable, ConnectionPool with ZConnectionPool] =
    ZLayer.scopedEnvironment {
      ConnectionPool
        .scoped(config, hooks)
        .map(pool => ZEnvironment(pool).add[ZConnectionPool](asZConnectionPool(pool)))
    }
}
