package zoi.pool.examples

import javax.sql.DataSource

import zio.{Console, ZIO, ZIOAppDefault}
import zoi.pool.{ConnectionPool, PoolConfig}

/**
 * Swapping the pool: anything that took a `DataSource` keeps taking one, and
 * only the layer that builds it changes.
 */
object DataSourceHandover extends ZIOAppDefault {

  /** A library that knows JDBC and nothing else. */
  private def countTables(source: DataSource): Int = {
    val connection = source.getConnection()
    try {
      val results = connection.getMetaData.getTables(null, null, "%", Array("TABLE"))
      try {
        var found = 0
        while (results.next()) found += 1
        found
      } finally results.close()
    } finally connection.close()
  }

  override def run =
    ZIO
      .serviceWithZIO[DataSource](source =>
        ZIO
          .attemptBlocking(countTables(source))
          .flatMap(count =>
            Console.printLine(s"$count tables, through a pool the library never heard of"),
          ),
      )
      .provide(
        ConnectionPool.dataSourceLayer(
          PoolConfig("jdbc:h2:mem:handover;DB_CLOSE_DELAY=-1", maximumPoolSize = 4),
        ),
      )
}
