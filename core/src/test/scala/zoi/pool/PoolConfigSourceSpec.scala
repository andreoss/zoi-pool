package zoi.pool

import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Config, ConfigProvider, Runtime, ZIO, durationInt}

/** Reading a pool configuration from the application's own config source. */
object PoolConfigSourceSpec extends ZIOSpecDefault {

  private def read(entries: (String, String)*) =
    ZIO
      .config(PoolConfig.configAt("zoi.pool"))
      .provide(Runtime.setConfigProvider(ConfigProvider.fromMap(entries.toMap)))

  def spec = suite("configuration source")(
    test("a url is enough, and every default comes through") {
      for {
        config <- read("zoi.pool.url" -> "jdbc:h2:mem:cfg")
      } yield assertTrue(
        config.url == "jdbc:h2:mem:cfg",
        config.poolName == "zoi-pool",
        config.maximumPoolSize == 10,
        config.minimumIdle.isEmpty,
        config.connectionTimeout == 30.seconds,
        config.idleTimeout == 10.minutes,
        config.failureTracking,
        !config.jmxEnabled,
      )
    },
    test("a missing url is reported, not defaulted") {
      assertZIO(read("zoi.pool.maximumPoolSize" -> "4").exit)(fails(anything))
    },
    test("every group of settings is read") {
      for {
        config <- read(
                    "zoi.pool.url"                    -> "jdbc:h2:mem:full",
                    "zoi.pool.username"               -> "scott",
                    "zoi.pool.password"               -> "tiger",
                    "zoi.pool.poolName"               -> "orders",
                    "zoi.pool.maximumPoolSize"        -> "20",
                    "zoi.pool.minimumIdle"            -> "5",
                    "zoi.pool.initialSize"            -> "2",
                    "zoi.pool.connectionTimeout"      -> "5s",
                    "zoi.pool.idleTimeout"            -> "2m",
                    "zoi.pool.maxLifetime"            -> "20m",
                    "zoi.pool.keepaliveTime"          -> "1m",
                    "zoi.pool.leakDetectionThreshold" -> "10s",
                    "zoi.pool.autoCommit"             -> "false",
                    "zoi.pool.transactionIsolation"   -> "TRANSACTION_SERIALIZABLE",
                    "zoi.pool.readOnly"               -> "true",
                    "zoi.pool.connectionInitSql"      -> "SELECT 1",
                    "zoi.pool.statementCacheSize"     -> "64",
                    "zoi.pool.failureTracking"        -> "false",
                    "zoi.pool.jmxEnabled"             -> "true",
                    "zoi.pool.jmxDomain"              -> "acme.pools",
                  )
      } yield assertTrue(
        config.username.contains("scott"),
        config.password.contains("tiger"),
        config.poolName == "orders",
        config.maximumPoolSize == 20,
        config.minimumIdle.contains(5),
        config.initialSize == 2,
        config.connectionTimeout == 5.seconds,
        config.idleTimeout == 2.minutes,
        config.maxLifetime == 20.minutes,
        config.keepaliveTime == 1.minute,
        config.leakDetectionThreshold == 10.seconds,
        !config.autoCommit,
        config.transactionIsolation.contains(TransactionIsolation.Serializable),
        config.readOnly,
        config.connectionInitSql.contains("SELECT 1"),
        config.statementCacheSize == 64,
        !config.failureTracking,
        config.jmxEnabled,
        config.jmxDomain == "acme.pools",
      )
    },
    test("an unknown isolation level is rejected by name") {
      for {
        outcome <- read(
                     "zoi.pool.url"                  -> "jdbc:h2:mem:bad",
                     "zoi.pool.transactionIsolation" -> "TRANSACTION_WISHFUL",
                   ).exit
      } yield assert(outcome)(fails(isSubtype[Config.Error](anything)))
    },
    test("a combination the pool cannot honour is rejected with its field named") {
      for {
        outcome <- read(
                     "zoi.pool.url"         -> "jdbc:h2:mem:bad",
                     "zoi.pool.idleTimeout" -> "40m",
                     "zoi.pool.maxLifetime" -> "10m",
                   ).flip
      } yield assertTrue(outcome.toString.contains("idleTimeout"))
    },
    test("connection properties are read as a table") {
      for {
        config <- read(
                    "zoi.pool.url"                          -> "jdbc:h2:mem:props",
                    "zoi.pool.connectionProperties.ssl"     -> "true",
                    "zoi.pool.connectionProperties.appName" -> "orders",
                  )
      } yield assertTrue(
        config.connectionProperties.size == 2,
        config.connectionProperties.get("ssl").contains("true"),
        config.connectionProperties.get("appName").contains("orders"),
      )
    },
    test("a pool can be built straight from configuration") {
      for {
        id     <- H2Backend.freshUrl
        answer <- ZIO.withConfigProvider(ConfigProvider.fromMap(Map("zoi.pool.url" -> id))) {
                    ZIO
                      .serviceWithZIO[ConnectionPool](pool =>
                        ZIO.scoped(
                          pool.connection.flatMap(c =>
                            ZIO.attemptBlocking(PoolTestSupport.queryInt(c, "SELECT 1")),
                          ),
                        ),
                      )
                      .provide(ConnectionPool.layerFromConfig())
                  }
      } yield assertTrue(answer == 1)
    },
  ) @@ withLiveRandom @@ timeout(60.seconds)
}
