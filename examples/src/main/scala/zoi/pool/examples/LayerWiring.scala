package zoi.pool.examples

import java.sql.Connection

import zio.{Console, ZIO, ZIOAppDefault, ZLayer}

import zoi.pool.{ConnectionPool, PoolConfig}

/** Wire the pool as a layer and build a service on top of it. */
object LayerWiring extends ZIOAppDefault {

  trait Widgets {
    def count: ZIO[Any, Throwable, Int]
  }

  final class LiveWidgets(pool: ConnectionPool) extends Widgets {
    def count: ZIO[Any, Throwable, Int] =
      ZIO.scoped(pool.connection.flatMap(connection => ZIO.attemptBlocking(readCount(connection))))

    private def readCount(connection: Connection): Int = {
      val statement = connection.createStatement()
      try {
        statement.execute("CREATE TABLE IF NOT EXISTS widget (id INT PRIMARY KEY)")
        val results = statement.executeQuery("SELECT COUNT(*) FROM widget")
        try {
          results.next()
          results.getInt(1)
        } finally results.close()
      } finally statement.close()
    }
  }

  object Widgets {
    val live: ZLayer[ConnectionPool, Nothing, Widgets] =
      ZLayer.fromFunction(new LiveWidgets(_: ConnectionPool))
  }

  private val pool =
    ConnectionPool.layer(PoolConfig("jdbc:h2:mem:layered;DB_CLOSE_DELAY=-1", maximumPoolSize = 4))

  override def run =
    ZIO
      .serviceWithZIO[Widgets](_.count)
      .flatMap(count => Console.printLine(s"$count widgets"))
      .provide(pool, Widgets.live)
}
