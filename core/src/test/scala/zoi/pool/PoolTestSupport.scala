package zoi.pool

import java.sql.{Connection, SQLException}

import zio.{Scope, ZIO}

/** Helpers shared by every pool spec. */
object PoolTestSupport {

  def withPool[A](config: PoolConfig)(
      use: ConnectionPool => ZIO[Any, Throwable, A],
  ): ZIO[Any, Throwable, A] =
    ZIO.scoped(ConnectionPool.scoped(config).flatMap(use))

  def borrow[A](pool: ConnectionPool)(use: Connection => A): ZIO[Any, Throwable, A] =
    ZIO.scoped(pool.connection.flatMap(connection => ZIO.attemptBlocking(use(connection))))

  def queryInt(connection: Connection, sql: String): Int = {
    val statement = connection.createStatement()
    try {
      val results = statement.executeQuery(sql)
      try {
        results.next()
        results.getInt(1)
      } finally results.close()
    } finally statement.close()
  }

  def scopedConnection(pool: ConnectionPool): ZIO[Scope, SQLException, Connection] =
    pool.connection
}
