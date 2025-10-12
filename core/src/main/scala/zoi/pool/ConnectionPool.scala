package zoi.pool

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

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

  /** The same pool seen as a plain JDBC `DataSource`. */
  def dataSource: DataSource

  /** How many connections the pool holds right now, and in which state. */
  def state: UIO[PoolState]

  /** Drops a borrowed connection the caller knows is bad. Idempotent. */
  def invalidate(connection: Connection): UIO[Unit]

  /** Stops handing out connections; current borrowers keep theirs. Idempotent. */
  def suspend: UIO[Unit]

  /** Undoes `suspend`, releasing everyone who blocked meanwhile. Idempotent. */
  def resume: UIO[Unit]
}

object ConnectionPool {

  /** Builds a pool that shuts down when the surrounding scope closes. */
  def scoped(
    config: PoolConfig,
    hooks: PoolHooks = PoolHooks.default,
  ): ZIO[Scope, SQLException, ConnectionPool] =
    ConnectionPoolLive.scoped(config, hooks)

  def layer(
    config: PoolConfig,
    hooks: PoolHooks = PoolHooks.default,
  ): ZLayer[Any, SQLException, ConnectionPool] =
    ZLayer.scoped(scoped(config, hooks))

  /** A `DataSource` layer, for consumers that take one. */
  def dataSourceLayer(
    config: PoolConfig,
    hooks: PoolHooks = PoolHooks.default,
  ): ZLayer[Any, SQLException, DataSource] =
    ZLayer.scoped(scoped(config, hooks).map(_.dataSource))

  /** Borrows a connection from the pool in the environment. */
  def connection: ZIO[ConnectionPool with Scope, SQLException, Connection] =
    ZIO.serviceWithZIO[ConnectionPool](_.connection)

  def state: ZIO[ConnectionPool, Nothing, PoolState] =
    ZIO.serviceWithZIO[ConnectionPool](_.state)

  def suspend: ZIO[ConnectionPool, Nothing, Unit] =
    ZIO.serviceWithZIO[ConnectionPool](_.suspend)

  def resume: ZIO[ConnectionPool, Nothing, Unit] =
    ZIO.serviceWithZIO[ConnectionPool](_.resume)
}
