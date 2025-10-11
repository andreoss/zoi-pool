package zoi.pool

import java.sql.{Connection, SQLException}

import zio.{Scope, UIO, ZIO, ZLayer}

/**
 * A JDBC connection pool whose lifetime is a `Scope`.
 *
 * `connection` hands out a connection for the duration of the caller's scope
 * and returns it when that scope closes; closing the pool's own scope shuts the
 * pool down and closes every connection it owns.
 */
trait ConnectionPool {

  /** Borrows a connection for the duration of the caller's scope. */
  def connection: ZIO[Scope, SQLException, Connection]

  /** How many connections the pool holds right now, and in which state. */
  def state: UIO[PoolState]
}

object ConnectionPool {

  /** Builds a pool that shuts down when the surrounding scope closes. */
  def scoped(config: PoolConfig): ZIO[Scope, SQLException, ConnectionPool] =
    for {
      factory <- ConnectionFactory.make(config)
      core    <- PoolCore.make[PooledConnection](config.poolName, config.maximumPoolSize)
      pool     = new ConnectionPoolLive(config, factory, core)
      _       <- ZIO.addFinalizer(pool.shutdown)
    } yield pool

  def layer(config: PoolConfig): ZLayer[Any, SQLException, ConnectionPool] =
    ZLayer.scoped(scoped(config))

  /** Borrows a connection from the pool in the environment. */
  def connection: ZIO[ConnectionPool with Scope, SQLException, Connection] =
    ZIO.serviceWithZIO[ConnectionPool](_.connection)

  def state: ZIO[ConnectionPool, Nothing, PoolState] =
    ZIO.serviceWithZIO[ConnectionPool](_.state)
}
