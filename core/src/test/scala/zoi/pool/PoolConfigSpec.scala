package zoi.pool

import zio.{Chunk, durationInt}
import zio.test.Assertion._
import zio.test._

object PoolConfigSpec extends ZIOSpecDefault {

  private val url = "jdbc:h2:mem:zp01"

  private def failsWith(field: String)(make: => PoolConfig) =
    assert(PoolConfig.validated(make).left.map(_.map(_.field)))(isLeft(contains(field)))

  def spec = suite("PoolConfig")(
    suite("defaults")(
      test("a url is the only thing a caller must supply") {
        val c = PoolConfig(url)
        assertTrue(
          c.url == url,
          c.username.isEmpty,
          c.password.isEmpty,
          c.driverClassName.isEmpty,
          c.connectionProperties.isEmpty,
          c.poolName == "zoi-pool",
        )
      },
      test("sizing defaults match the documented values") {
        val c = PoolConfig(url)
        assertTrue(
          c.maximumPoolSize == 10,
          c.minimumIdle.isEmpty,
          c.effectiveMinimumIdle == 10,
          c.initialSize == 0,
        )
      },
      test("timeout defaults match the documented values") {
        val c = PoolConfig(url)
        assertTrue(
          c.connectionTimeout == 30.seconds,
          c.validationTimeout == 5.seconds,
          c.idleTimeout == 10.minutes,
          c.maxLifetime == 30.minutes,
          c.keepaliveTime == 0.seconds,
          c.leakDetectionThreshold == 0.seconds,
          c.aliveBypassWindow == 500.millis,
          c.shutdownTimeout == 30.seconds,
        )
      },
      test("connection-state and feature defaults match the documented values") {
        val c = PoolConfig(url)
        assertTrue(
          c.autoCommit,
          c.transactionIsolation.isEmpty,
          !c.readOnly,
          c.catalog.isEmpty,
          c.schema.isEmpty,
          c.connectionInitSql.isEmpty,
          c.connectionTestQuery.isEmpty,
          c.statementCacheSize == 0,
          c.failureTracking,
          !c.jmxEnabled,
          !c.leakDetectionEnabled,
          !c.keepaliveEnabled,
          !c.statementCacheEnabled,
        )
      },
    ),
    suite("validation")(
      test("an empty url is rejected")(failsWith("url")(PoolConfig(""))),
      test("a blank url is rejected")(failsWith("url")(PoolConfig("   "))),
      test("maximumPoolSize must be positive")(
        failsWith("maximumPoolSize")(PoolConfig(url, maximumPoolSize = 0)),
      ),
      test("minimumIdle must not exceed maximumPoolSize")(
        failsWith("minimumIdle")(PoolConfig(url, maximumPoolSize = 4, minimumIdle = Some(5))),
      ),
      test("minimumIdle must not be negative")(
        failsWith("minimumIdle")(PoolConfig(url, minimumIdle = Some(-1))),
      ),
      test("initialSize must not exceed maximumPoolSize")(
        failsWith("initialSize")(PoolConfig(url, maximumPoolSize = 2, initialSize = 3)),
      ),
      test("connectionTimeout must be positive")(
        failsWith("connectionTimeout")(PoolConfig(url, connectionTimeout = 0.seconds)),
      ),
      test("validationTimeout must be positive")(
        failsWith("validationTimeout")(PoolConfig(url, validationTimeout = 0.seconds)),
      ),
      test("idleTimeout must not be negative")(
        failsWith("idleTimeout")(PoolConfig(url, idleTimeout = -1.seconds)),
      ),
      test("statementCacheSize must not be negative")(
        failsWith("statementCacheSize")(PoolConfig(url, statementCacheSize = -1)),
      ),
      test("keepaliveTime must be shorter than maxLifetime")(
        failsWith("keepaliveTime")(
          PoolConfig(url, keepaliveTime = 10.minutes, maxLifetime = 5.minutes),
        ),
      ),
      test("idleTimeout must be shorter than maxLifetime")(
        failsWith("idleTimeout")(PoolConfig(url, idleTimeout = 40.minutes)),
      ),
      test("a blank poolName is rejected")(failsWith("poolName")(PoolConfig(url, poolName = " "))),
      test("every violation is reported, not just the first") {
        val errors = PoolConfig.validated(PoolConfig("", maximumPoolSize = -1, initialSize = -1))
        assert(errors.left.map(_.map(_.field)))(
          isLeft(hasSameElements(Chunk("url", "maximumPoolSize", "initialSize"))),
        )
      },
      test("an invalid copy is rejected too") {
        failsWith("maximumPoolSize")(PoolConfig(url).copy(maximumPoolSize = 0))
      },
      test("a valid config is returned unchanged") {
        val c = PoolConfig(url, maximumPoolSize = 3)
        assert(PoolConfig.validated(c))(isRight(equalTo(c)))
      },
      test("the failure message names every offending field") {
        val thrown = scala.util.Try(PoolConfig("", maximumPoolSize = 0)).failed.get
        assertTrue(
          thrown.isInstanceOf[IllegalArgumentException],
          thrown.getMessage.contains("url"),
          thrown.getMessage.contains("maximumPoolSize"),
        )
      },
    ),
    suite("derived values")(
      test("minimumIdle defaults to maximumPoolSize") {
        assertTrue(PoolConfig(url, maximumPoolSize = 7).effectiveMinimumIdle == 7)
      },
      test("an explicit minimumIdle wins") {
        assertTrue(PoolConfig(url, maximumPoolSize = 7, minimumIdle = Some(2)).effectiveMinimumIdle == 2)
      },
      test("zero-valued durations read as disabled features") {
        val c = PoolConfig(
          url,
          keepaliveTime = 1.minute,
          leakDetectionThreshold = 2.seconds,
          statementCacheSize = 8,
        )
        assertTrue(c.keepaliveEnabled, c.leakDetectionEnabled, c.statementCacheEnabled)
      },
      test("credentials never appear in the rendered config") {
        val c = PoolConfig(url, username = Some("scott"), password = Some("tiger"))
        assertTrue(!c.toString.contains("tiger"), c.toString.contains("zoi-pool"))
      },
    ),
    suite("TransactionIsolation")(
      test("every level maps to its JDBC constant") {
        assertTrue(
          TransactionIsolation.NoTransactions.jdbcLevel == java.sql.Connection.TRANSACTION_NONE,
          TransactionIsolation.ReadUncommitted.jdbcLevel == java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,
          TransactionIsolation.ReadCommitted.jdbcLevel == java.sql.Connection.TRANSACTION_READ_COMMITTED,
          TransactionIsolation.RepeatableRead.jdbcLevel == java.sql.Connection.TRANSACTION_REPEATABLE_READ,
          TransactionIsolation.Serializable.jdbcLevel == java.sql.Connection.TRANSACTION_SERIALIZABLE,
        )
      },
      test("names parse in the JDBC and the bare form") {
        assertTrue(
          TransactionIsolation.fromName("TRANSACTION_READ_COMMITTED").contains(TransactionIsolation.ReadCommitted),
          TransactionIsolation.fromName("read_committed").contains(TransactionIsolation.ReadCommitted),
          TransactionIsolation.fromName("ReadCommitted").contains(TransactionIsolation.ReadCommitted),
          TransactionIsolation.fromName("nonsense").isEmpty,
        )
      },
      test("levels parse back from their JDBC constant") {
        assertTrue(
          TransactionIsolation.fromJdbcLevel(8).contains(TransactionIsolation.Serializable),
          TransactionIsolation.fromJdbcLevel(3).isEmpty,
        )
      },
      test("name round-trips through fromName") {
        check(Gen.fromIterable(TransactionIsolation.all)) { level =>
          assertTrue(TransactionIsolation.fromName(level.name).contains(level))
        }
      },
    ),
  )
}
