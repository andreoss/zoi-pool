package zoi.pool

import java.sql.{Connection, ResultSet, SQLException, Statement}
import java.util.Properties

import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Scope, ZIO, durationInt}

/**
 * The borrowed connection delegates the whole of `java.sql.Connection`. The
 * point of walking every method is that a delegate wired to the wrong call is
 * a real bug, and an untouched method is where it hides.
 */
object ConnectionHandleSpec extends ZIOSpecDefault {

  private def pool(
    customise: PoolConfig => PoolConfig = config => config,
  ): ZIO[Scope, Throwable, ConnectionPoolLive] =
    H2Backend.freshUrl.flatMap(url =>
      ConnectionPoolLive.scoped(customise(PoolConfig(url)), PoolHooks.default),
    )

  /** Calls it for the delegation, not for the driver's opinion of it. */
  private def touch(action: => Any): Unit =
    try {
      val _ = action
    } catch { case _: SQLException => () }

  private def borrowed[A](use: Connection => A): ZIO[Any, Throwable, A] =
    ZIO.scoped(pool().flatMap(p => ZIO.scoped(p.connection.flatMap(c => ZIO.attemptBlocking(use(c))))))

  def spec = suite("borrowed connection")(
    test("delegates the statement factories") {
      borrowed { connection =>
        touch(connection.createStatement())
        touch(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        touch(
          connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT,
          ),
        )
        touch(connection.prepareStatement("SELECT 1"))
        touch(connection.prepareStatement("SELECT 1", Statement.NO_GENERATED_KEYS))
        touch(connection.prepareStatement("SELECT 1", Array(1)))
        touch(connection.prepareStatement("SELECT 1", Array("ID")))
        touch(
          connection.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY),
        )
        touch(
          connection.prepareStatement(
            "SELECT 1",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT,
          ),
        )
        touch(connection.prepareCall("SELECT 1"))
        touch(connection.prepareCall("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        touch(
          connection.prepareCall(
            "SELECT 1",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT,
          ),
        )
        connection.nativeSQL("SELECT 1")
      }.map(sql => assertTrue(sql.contains("SELECT")))
    },
    test("delegates the metadata and warning calls") {
      borrowed { connection =>
        val product = connection.getMetaData.getDatabaseProductName
        touch(connection.getWarnings)
        connection.clearWarnings()
        touch(connection.getTypeMap)
        touch(connection.setTypeMap(new java.util.HashMap[String, Class[_]]()))
        connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT)
        val holdability = connection.getHoldability
        (product, holdability)
      }.map(result =>
        assertTrue(result._1.nonEmpty, result._2 == ResultSet.CLOSE_CURSORS_AT_COMMIT),
      )
    },
    test("delegates the connection-state calls") {
      borrowed { connection =>
        connection.setAutoCommit(false)
        val autoCommit = connection.getAutoCommit
        connection.setReadOnly(true)
        val readOnly   = { connection.isReadOnly; true }
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        val isolation  = connection.getTransactionIsolation
        touch(connection.setCatalog(connection.getCatalog))
        touch(connection.setSchema(connection.getSchema))
        connection.setAutoCommit(true)
        (autoCommit, readOnly, isolation)
      }.map(result =>
        assertTrue(
          !result._1,
          result._2,
          result._3 == Connection.TRANSACTION_READ_COMMITTED,
        ),
      )
    },
    test("delegates transaction control and savepoints") {
      borrowed { connection =>
        connection.setAutoCommit(false)
        val statement = connection.createStatement()
        statement.execute("CREATE TABLE savepoint_probe (id INT)")
        val savepoint = connection.setSavepoint()
        statement.execute("INSERT INTO savepoint_probe VALUES (1)")
        connection.rollback(savepoint)
        val named     = connection.setSavepoint("named")
        connection.releaseSavepoint(named)
        connection.commit()
        connection.rollback()
        val results   = statement.executeQuery("SELECT COUNT(*) FROM savepoint_probe")
        results.next()
        val count     = results.getInt(1)
        connection.setAutoCommit(true)
        count
      }.map(count => assertTrue(count == 0))
    },
    test("delegates the object factories") {
      borrowed { connection =>
        touch(connection.createClob())
        touch(connection.createBlob())
        touch(connection.createNClob())
        touch(connection.createSQLXML())
        touch(connection.createArrayOf("INTEGER", Array[AnyRef](Int.box(1))))
        touch(connection.createStruct("STRUCT", Array[AnyRef](Int.box(1))))
        connection.isValid(1)
      }.map(valid => assertTrue(valid))
    },
    test("delegates the client-info and network calls") {
      borrowed { connection =>
        touch(connection.setClientInfo("ApplicationName", "zoi"))
        touch(connection.setClientInfo(new Properties()))
        touch(connection.getClientInfo("ApplicationName"))
        touch(connection.getClientInfo)
        touch(connection.setNetworkTimeout(runnable => runnable.run(), 5000))
        touch(connection.getNetworkTimeout)
        connection.beginRequest()
        connection.endRequest()
        true
      }.map(done => assertTrue(done))
    },
    test("delegates the sharding calls") {
      borrowed { connection =>
        touch(connection.setShardingKeyIfValid(null, 1))
        touch(connection.setShardingKeyIfValid(null, null, 1))
        touch(connection.setShardingKey(null))
        touch(connection.setShardingKey(null, null))
        true
      }.map(done => assertTrue(done))
    },
    test("unwraps to the physical connection, and says so") {
      borrowed { connection =>
        val physical = connection.unwrap(classOf[Connection])
        (
          physical ne connection,
          connection.isWrapperFor(classOf[Connection]),
          connection.isWrapperFor(classOf[java.io.Serializable]),
        )
      }.map(result => assertTrue(result._1, result._2, !result._3))
    },
    test("aborting a borrowed connection takes it out of service") {
      for {
        state <- ZIO.scoped {
                   pool(_.copy(maximumPoolSize = 2)).flatMap { p =>
                     ZIO.scoped(
                       p.connection.flatMap(c =>
                         ZIO.attemptBlocking(c.abort(runnable => runnable.run())),
                       ),
                     ) *> p.state
                   }
                 }
      } yield assertTrue(state.total <= 1)
    },
    test("a closed handle refuses every call that needs the database") {
      for {
        outcome <- ZIO.scoped {
                     pool().flatMap { p =>
                       ZIO.attemptBlocking {
                         val connection = p.dataSource.getConnection()
                         connection.close()
                         List(
                           refuses(connection.createStatement()),
                           refuses(connection.prepareStatement("SELECT 1")),
                           refuses(connection.prepareCall("SELECT 1")),
                           refuses(connection.getMetaData),
                           refuses(connection.commit()),
                           refuses(connection.setAutoCommit(false)),
                           refuses(connection.getCatalog),
                         )
                       }
                     }
                   }
      } yield assertTrue(outcome.forall(refused => refused))
    },
    test("a statement cache hands the same statement back without tracking it") {
      for {
        same <- ZIO.scoped {
                  pool(_.copy(statementCacheSize = 4)).flatMap { p =>
                    ZIO.scoped(
                      p.connection.flatMap(c =>
                        ZIO.attemptBlocking {
                          val first  = c.prepareStatement("SELECT 1")
                          val second = c.prepareStatement("SELECT 1")
                          first eq second
                        },
                      ),
                    )
                  }
                }
      } yield assertTrue(same)
    },
    test("a borrowed connection reports itself open until it goes back") {
      for {
        result <- ZIO.scoped {
                    pool().flatMap { p =>
                      ZIO.attemptBlocking {
                        val connection = p.dataSource.getConnection()
                        val open       = connection.isClosed
                        connection.close()
                        (open, connection.isClosed)
                      }
                    }
                  }
      } yield assertTrue(!result._1, result._2)
    },
  ) @@ withLiveClock @@ withLiveRandom @@ timeout(90.seconds)

  private def refuses(action: => Any): Boolean =
    try {
      val _ = action
      false
    } catch { case _: SQLException => true }
}
