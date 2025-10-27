package zoi.pool.examples

import zio.{Console, ConfigProvider, ZIO, ZIOAppDefault}
import zoi.pool.ConnectionPool

/** Build the pool from the application's own configuration. */
object Configured extends ZIOAppDefault {

  private val settings = ConfigProvider.fromMap(
    Map(
      "zoi.pool.url"                    -> "jdbc:h2:mem:configured;DB_CLOSE_DELAY=-1",
      "zoi.pool.maximumPoolSize"        -> "8",
      "zoi.pool.minimumIdle"            -> "2",
      "zoi.pool.connectionTimeout"      -> "10s",
      "zoi.pool.leakDetectionThreshold" -> "30s",
    ),
  )

  override def run =
    ZIO.withConfigProvider(settings) {
      ZIO
        .serviceWithZIO[ConnectionPool](pool =>
          ZIO.scoped(pool.connection) *> pool.state.flatMap(state =>
            Console.printLine(s"configured pool holds ${state.total}"),
          ),
        )
        .provide(ConnectionPool.layerFromConfig())
    }
}
