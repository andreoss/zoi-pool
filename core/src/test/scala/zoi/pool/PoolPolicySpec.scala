package zoi.pool

import java.sql.Connection

import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Promise, Scope, ZIO, durationInt}

/** Lifecycle policies, driven by the test clock so timing is exact. */
object PoolPolicySpec extends ZIOSpecDefault {

  private val backend = H2Backend

  private def pool(config: PoolConfig): ZIO[Scope, Throwable, ConnectionPoolLive] =
    ConnectionPoolLive.scoped(config, PoolHooks.default)

  private def physical(connection: Connection): Connection =
    connection.unwrap(classOf[Connection])

  def spec = suite("pool policies")(
    suite("prefill")(
      test("initialSize opens connections before the pool is handed over") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped(
            pool(backend.config(url).copy(maximumPoolSize = 5, initialSize = 3))
              .flatMap(_.state),
          )
        } yield assertTrue(state.total == 3, state.idle == 3, state.active == 0)
      },
      test("maintenance tops the pool up to minimumIdle") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(
              backend.config(url).copy(maximumPoolSize = 6, minimumIdle = Some(2)),
            ).flatMap(p => p.maintain *> p.state)
          }
        } yield assertTrue(state.idle == 2, state.total == 2)
      },
      test("maintenance never opens past the cap") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 2))
              .flatMap(p => p.maintain *> p.maintain *> p.state)
          }
        } yield assertTrue(state.total == 2)
      },
    ),
    suite("idle timeout")(
      test("retires a connection idle for longer than idleTimeout") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              maximumPoolSize = 4,
              minimumIdle = Some(0),
              idleTimeout = 1.minute,
            )
          states <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                _      <- ZIO.scoped(p.connection)
                before <- p.state
                _      <- TestClock.adjust(2.minutes)
                _      <- p.maintain
                after  <- p.state
              } yield (before, after)
            }
          }
        } yield assertTrue(states._1.idle == 1, states._2.idle == 0, states._2.total == 0)
      },
      test("keeps minimumIdle connections however long they sit") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              maximumPoolSize = 4,
              minimumIdle = Some(2),
              initialSize = 2,
              idleTimeout = 1.minute,
            )
          state <- ZIO.scoped {
            pool(config).flatMap { p =>
              TestClock.adjust(10.minutes) *> p.maintain *> p.state
            }
          }
        } yield assertTrue(state.idle == 2, state.total == 2)
      },
    ),
    suite("max lifetime")(
      test("retires an idle connection past its lifetime") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              maximumPoolSize = 4,
              minimumIdle = Some(0),
              idleTimeout = 1.minute,
              maxLifetime = 5.minutes,
            )
          state <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                _     <- ZIO.scoped(p.connection)
                _     <- TestClock.adjust(6.minutes)
                _     <- p.maintain
                state <- p.state
              } yield state
            }
          }
        } yield assertTrue(state.total == 0)
      },
      test("never hands out a connection past its lifetime") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              maximumPoolSize = 2,
              idleTimeout = 1.minute,
              maxLifetime = 5.minutes,
            )
          result <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                first  <- ZIO.scoped(p.connection.map(physical))
                _      <- TestClock.adjust(6.minutes)
                second <- ZIO.scoped(p.connection.map(physical))
                closed <- ZIO.attemptBlocking(first.isClosed)
              } yield (first eq second, closed)
            }
          }
        } yield assertTrue(!result._1, result._2)
      },
    ),
    suite("validation")(
      test("a connection that died while idle is replaced, not handed out") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = 2, aliveBypassWindow = 1.second)
          result <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                first  <- ZIO.scoped(p.connection.map(physical))
                _      <- ZIO.attemptBlocking(first.close())
                _      <- TestClock.adjust(5.seconds)
                second <- ZIO.scoped(p.connection.map(physical))
                alive  <- ZIO.attemptBlocking(second.isValid(1))
              } yield (first eq second, alive)
            }
          }
        } yield assertTrue(!result._1, result._2)
      },
      test("the alive-bypass window skips validation on a hot connection") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = 2, aliveBypassWindow = 1.minute)
          result <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                first  <- ZIO.scoped(p.connection.map(physical))
                second <- ZIO.scoped(p.connection.map(physical))
              } yield first eq second
            }
          }
        } yield assertTrue(result)
      },
      test("a test query that cannot run keeps bad connections out of the pool") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              maximumPoolSize = 2,
              connectionTimeout = 200.millis,
              connectionTestQuery = Some("SELECT 1 FROM nonexistent_table_zoi"),
            )
          result <- ZIO.scoped {
            pool(config).flatMap(p => ZIO.scoped(p.connection).either <*> p.state)
          }
        } yield assert(result._1)(isLeft(isSubtype[ConnectionCreationException](anything))) &&
          assertTrue(result._2.total == 0, result._2.idle == 0)
      } @@ withLiveClock,
    ),
    suite("keepalive")(
      test("probes an idle connection and retires it when it is dead") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              maximumPoolSize = 2,
              minimumIdle = Some(0),
              keepaliveTime = 1.minute,
              idleTimeout = 10.minutes,
              maxLifetime = 30.minutes,
            )
          state <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                raw   <- ZIO.scoped(p.connection.map(physical))
                _     <- ZIO.attemptBlocking(raw.close())
                _     <- TestClock.adjust(2.minutes)
                _     <- p.maintain
                state <- p.state
              } yield state
            }
          }
        } yield assertTrue(state.total == 0, state.idle == 0)
      },
      test("keeps a healthy idle connection alive") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              maximumPoolSize = 2,
              minimumIdle = Some(0),
              keepaliveTime = 1.minute,
              idleTimeout = 10.minutes,
              maxLifetime = 30.minutes,
            )
          state <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                _     <- ZIO.scoped(p.connection)
                _     <- TestClock.adjust(2.minutes)
                _     <- p.maintain
                state <- p.state
              } yield state
            }
          }
        } yield assertTrue(state.total == 1, state.idle == 1)
      },
    ),
    suite("connection state")(
      test("applies the configured state to every connection") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              autoCommit = false,
              transactionIsolation = Some(TransactionIsolation.Serializable),
            )
          result <- ZIO.scoped {
            pool(config).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c =>
                  ZIO.attemptBlocking((c.getAutoCommit, c.getTransactionIsolation)),
                ),
              )
            }
          }
        } yield assertTrue(
          !result._1,
          result._2 == TransactionIsolation.Serializable.jdbcLevel,
        )
      },
      test("runs connectionInitSql once per connection") {
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              connectionInitSql = Some("CREATE TABLE IF NOT EXISTS zoi_init (id INT)"),
            )
          answer <- ZIO.scoped {
            pool(config).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c =>
                  ZIO.attemptBlocking(
                    PoolTestSupport.queryInt(c, "SELECT COUNT(*) FROM zoi_init"),
                  ),
                ),
              )
            }
          }
        } yield assertTrue(answer == 0)
      },
      test("state a borrower changed is restored before the next borrow") {
        for {
          url    <- backend.freshUrl
          result <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 1)).flatMap { p =>
              for {
                _      <- ZIO.scoped(
                  p.connection.flatMap(c => ZIO.attemptBlocking(c.setAutoCommit(false))),
                )
                second <- ZIO.scoped(
                  p.connection.flatMap(c => ZIO.attemptBlocking(c.getAutoCommit)),
                )
              } yield second
            }
          }
        } yield assertTrue(result)
      },
      test("a clean borrow is not marked dirty and a changed one is") {
        for {
          url    <- backend.freshUrl
          result <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 1)).flatMap { p =>
              for {
                clean <- ZIO.scoped(
                  p.connection.flatMap(c =>
                    ZIO.attemptBlocking {
                      PoolTestSupport.queryInt(c, "SELECT 1")
                      c.unwrap(classOf[Connection])
                    } *> ZIO.succeed(dirtyOf(c)),
                  ),
                )
                dirty <- ZIO.scoped(
                  p.connection.flatMap(c =>
                    ZIO.attemptBlocking(c.setReadOnly(true)) *>
                      ZIO.succeed(dirtyOf(c)),
                  ),
                )
              } yield (clean, dirty)
            }
          }
        } yield assertTrue(!result._1, result._2)
      },
    ),
    suite("leak detection")(
      test("reports a connection held past the threshold, once") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(leakDetectionThreshold = 1.minute)
          entries <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                held  <- Promise.make[Nothing, Unit]
                fiber <- ZIO
                  .scoped(p.connection *> held.succeed(()) *> ZIO.never)
                  .fork
                _     <- held.await
                _     <- TestClock.adjust(2.minutes)
                _     <- p.maintain *> p.maintain
                logs  <- ZTestLogger.logOutput
                _     <- fiber.interrupt
              } yield logs
            }
          }
          warnings = entries.filter(_.logLevel == zio.LogLevel.Warning)
        } yield assertTrue(
          warnings.size == 1,
          warnings.head.message().contains("may be a leak"),
          !warnings.head.message().contains("jdbc:"),
        )
      },
      test("says nothing about a connection returned in time") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(leakDetectionThreshold = 1.minute)
          entries <- ZIO.scoped {
            pool(config).flatMap { p =>
              ZIO.scoped(p.connection) *>
                TestClock.adjust(2.minutes) *>
                p.maintain *>
                ZTestLogger.logOutput
            }
          }
        } yield assertTrue(entries.count(_.logLevel == zio.LogLevel.Warning) == 0)
      },
    ),
    suite("statement cache")(
      test("returns the same statement for the same sql") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(statementCacheSize = 4)
          result <- ZIO.scoped {
            pool(config).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c =>
                  ZIO.attemptBlocking {
                    val first  = c.prepareStatement("SELECT 1")
                    val second = c.prepareStatement("SELECT 1")
                    (first eq second, first.isClosed)
                  },
                ),
              )
            }
          }
        } yield assertTrue(result._1, !result._2)
      },
      test("survives a return and is reused by the next borrower") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = 1, statementCacheSize = 4)
          result <- ZIO.scoped {
            pool(config).flatMap { p =>
              for {
                first  <- ZIO.scoped(
                  p.connection.flatMap(c => ZIO.attemptBlocking(c.prepareStatement("SELECT 1"))),
                )
                second <- ZIO.scoped(
                  p.connection.flatMap(c => ZIO.attemptBlocking(c.prepareStatement("SELECT 1"))),
                )
              } yield first eq second
            }
          }
        } yield assertTrue(result)
      },
      test("evicts the least recently used statement past capacity") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(statementCacheSize = 2)
          result <- ZIO.scoped {
            pool(config).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c =>
                  ZIO.attemptBlocking {
                    val first = c.prepareStatement("SELECT 1")
                    c.prepareStatement("SELECT 2")
                    c.prepareStatement("SELECT 3")
                    val again = c.prepareStatement("SELECT 1")
                    (first eq again, first.isClosed)
                  },
                ),
              )
            }
          }
        } yield assertTrue(!result._1, result._2)
      },
      test("closes cached statements when the connection goes") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = 1, statementCacheSize = 2)
          statement <- ZIO.scoped {
            pool(config).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c => ZIO.attemptBlocking(c.prepareStatement("SELECT 1"))),
              )
            }
          }
          closed    <- ZIO.attemptBlocking(statement.isClosed)
        } yield assertTrue(closed)
      },
    ),
  ) @@ withLiveRandom @@ sequential @@ timeout(120.seconds)

  private def dirtyOf(connection: Connection): Boolean =
    connection.asInstanceOf[ConnectionHandle].pooled.stateDirty
}
