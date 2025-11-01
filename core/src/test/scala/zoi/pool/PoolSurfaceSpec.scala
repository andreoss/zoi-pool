package zoi.pool

import java.io.PrintWriter
import java.lang.management.ManagementFactory
import java.sql.SQLFeatureNotSupportedException
import javax.sql.DataSource

import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Scope, ZIO, durationInt}

/** The published surfaces: the layers, the DataSource and the management bean. */
object PoolSurfaceSpec extends ZIOSpecDefault {

  private val backend = H2Backend

  private def live(
    customise: PoolConfig => PoolConfig = config => config,
  ): ZIO[Scope, Throwable, ConnectionPoolLive] =
    backend.freshUrl.flatMap(url =>
      ConnectionPoolLive.scoped(customise(PoolConfig(url)), PoolHooks.default),
    )

  def spec = suite("published surfaces")(
    suite("layers")(
      test("the pool layer provides a pool that works and then closes") {
        for {
          url    <- backend.freshUrl
          answer <- ZIO
                      .scoped(
                        ConnectionPool.connection.flatMap(c =>
                          ZIO.attemptBlocking(PoolTestSupport.queryInt(c, "SELECT 1")),
                        ),
                      )
                      .provide(ConnectionPool.layer(PoolConfig(url)))
        } yield assertTrue(answer == 1)
      },
      test("the accessors reach the pool in the environment") {
        for {
          url     <- backend.freshUrl
          results <- (for {
                       before <- ConnectionPool.state
                       _      <- ConnectionPool.suspend
                       during <- ConnectionPool.state
                       _      <- ConnectionPool.resume
                       after  <- ConnectionPool.state
                     } yield (before, during, after))
                       .provide(ConnectionPool.layer(PoolConfig(url)))
        } yield assertTrue(
          !results._1.suspended,
          results._2.suspended,
          !results._3.suspended,
        )
      },
      test("the DataSource layer provides only a DataSource") {
        for {
          url    <- backend.freshUrl
          answer <- ZIO
                      .serviceWithZIO[DataSource](source =>
                        ZIO.attemptBlocking {
                          val connection = source.getConnection()
                          try PoolTestSupport.queryInt(connection, "SELECT 1")
                          finally connection.close()
                        },
                      )
                      .provide(ConnectionPool.dataSourceLayer(PoolConfig(url)))
        } yield assertTrue(answer == 1)
      },
    ),
    suite("data source")(
      test("carries a log writer and a login timeout for callers that set them") {
        for {
          result <- ZIO.scoped {
                      live().flatMap { pool =>
                        ZIO.attemptBlocking {
                          val source = pool.dataSource
                          val writer = new PrintWriter(System.out)
                          source.setLogWriter(writer)
                          source.setLoginTimeout(7)
                          (source.getLogWriter eq writer, source.getLoginTimeout)
                        }
                      }
                    }
        } yield assertTrue(result._1, result._2 == 7)
      },
      test("unwraps to itself and to nothing else") {
        for {
          result <- ZIO.scoped {
                      live().flatMap { pool =>
                        ZIO.attemptBlocking {
                          val source = pool.dataSource
                          (
                            source.isWrapperFor(classOf[DataSource]),
                            source.unwrap(classOf[DataSource]) eq source,
                            scala.util.Try(source.unwrap(classOf[String])).isFailure,
                          )
                        }
                      }
                    }
        } yield assertTrue(result._1, result._2, result._3)
      },
      test("has no parent logger to offer") {
        for {
          outcome <- ZIO.scoped {
                       live().flatMap(pool => ZIO.attemptBlocking(pool.dataSource.getParentLogger).either)
                     }
        } yield assert(outcome)(isLeft(isSubtype[SQLFeatureNotSupportedException](anything)))
      },
    ),
    suite("connection factory")(
      test("a driver class that is not there fails the pool, not the process") {
        for {
          url     <- backend.freshUrl
          outcome <- ZIO
                       .scoped(
                         ConnectionPool.scoped(
                           PoolConfig(url, driverClassName = Some("com.example.NoSuchDriver")),
                         ),
                       )
                       .either
        } yield assert(outcome)(isLeft(isSubtype[ConnectionCreationException](anything)))
      },
      test("a url no driver understands fails the borrow") {
        for {
          outcome <- ZIO
                       .scoped(
                         ConnectionPool
                           .scoped(PoolConfig("jdbc:nosuchdatabase:memory:x", connectionTimeout = 200.millis))
                           .flatMap(pool => ZIO.scoped(pool.connection)),
                       )
                       .either
        } yield assert(outcome)(isLeft(isSubtype[ConnectionCreationException](anything)))
      } @@ withLiveClock,
      test("the configured driver class is loaded when it exists") {
        for {
          url    <- backend.freshUrl
          answer <- ZIO.scoped {
                      ConnectionPool
                        .scoped(PoolConfig(url, driverClassName = Some("org.h2.Driver")))
                        .flatMap(pool =>
                          ZIO.scoped(
                            pool.connection.flatMap(c =>
                              ZIO.attemptBlocking(PoolTestSupport.queryInt(c, "SELECT 1")),
                            ),
                          ),
                        )
                    }
        } yield assertTrue(answer == 1)
      },
    ),
    suite("management bean")(
      test("every attribute the bean publishes is readable") {
        val server = ManagementFactory.getPlatformMBeanServer
        for {
          url     <- backend.freshUrl
          config   = PoolConfig(
                       url,
                       poolName = "surface",
                       maximumPoolSize = 6,
                       minimumIdle = Some(2),
                       jmxEnabled = true,
                     )
          name     = PoolManagement.objectName(config)
          readings <- ZIO.scoped {
                        ConnectionPoolLive
                          .scoped(config, PoolHooks(metrics = PoolMetrics.recording))
                          .flatMap { pool =>
                            ZIO.scoped(pool.connection) *> ZIO.attemptBlocking {
                              List(
                                "PoolName",
                                "MaximumPoolSize",
                                "MinimumIdle",
                                "ActiveConnections",
                                "IdleConnections",
                                "TotalConnections",
                                "BorrowersWaiting",
                                "ConnectionsCreated",
                                "ConnectionsClosed",
                                "ConnectionsRetired",
                                "Acquires",
                                "AcquireTimeouts",
                                "LeaksSuspected",
                                "MeanAcquireMillis",
                                "Suspended",
                                "Shutdown",
                              ).map(attribute => attribute -> server.getAttribute(name, attribute))
                            }
                          }
                      }
        } yield assertTrue(
          readings.size == 16,
          readings.forall(_._2 != null),
          readings.toMap.apply("PoolName") == "surface",
          readings.toMap.apply("MinimumIdle") == Int.box(2),
        )
      },
      test("registering twice leaves one bean and unregisters cleanly") {
        val server = ManagementFactory.getPlatformMBeanServer
        for {
          url    <- backend.freshUrl
          config  = PoolConfig(url, poolName = "surface-twice", jmxEnabled = true)
          name    = PoolManagement.objectName(config)
          during <- ZIO.scoped {
                      ConnectionPoolLive.scoped(config, PoolHooks.default) *>
                        ConnectionPoolLive.scoped(config, PoolHooks.default) *>
                        ZIO.attemptBlocking(server.isRegistered(name))
                    }
          after  <- ZIO.attemptBlocking(server.isRegistered(name))
        } yield assertTrue(during, !after)
      },
    ),
  ) @@ withLiveRandom @@ sequential @@ timeout(90.seconds)
}
