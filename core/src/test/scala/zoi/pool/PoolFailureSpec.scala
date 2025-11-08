package zoi.pool

import java.sql.{SQLException, SQLTransientConnectionException}

import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Promise, Scope, ZIO, durationInt}

import zoi.pool.SqlExceptionClassification._

/** Failure semantics, invalidation, suspension and shutdown races. */
object PoolFailureSpec extends ZIOSpecDefault {

  private val backend = H2Backend

  private def pool(
      config: PoolConfig,
      hooks: PoolHooks = PoolHooks.default,
  ): ZIO[Scope, Throwable, ConnectionPoolLive] = ConnectionPoolLive.scoped(config, hooks)

  private def fatalSql = new SQLException("gone", "08006")

  def spec = suite("failure semantics")(
    suite("classification")(
      test("SQLState class 08 is a connection failure") {
        assertTrue(
          default(new SQLException("x", "08006")) == Fatal,
          default(new SQLException("x", "08S01")) == Fatal,
          default(new SQLException("x", "08003")) == Fatal,
        )
      },
      test("an administrative shutdown is a connection failure") {
        assertTrue(default(new SQLException("x", "57P01")) == Fatal)
      },
      test("an ordinary constraint violation leaves the connection usable") {
        assertTrue(
          default(new SQLException("duplicate key", "23505")) == Recoverable,
          default(new SQLException("syntax", "42601")) == Recoverable,
          default(new RuntimeException("not sql")) == Recoverable,
        )
      },
      test("the connection-shaped exception types speak for themselves") {
        assertTrue(default(new SQLTransientConnectionException("x")) == Fatal)
      },
      test("a chained cause is read too") {
        val outer = new SQLException("wrapper", "99999")
        outer.setNextException(new SQLException("inner", "08006"))
        assertTrue(default(outer) == Fatal)
      },
      test("a vendor error code can be fatal without a state") {
        assertTrue(default(new SQLException("x", null, 500150)) == Fatal)
      },
      test("an override decides before the default does") {
        val hooks = PoolHooks(classify = _ => Some(Recoverable))
        assertTrue(
          hooks.classification(new SQLException("x", "08006")) == Recoverable,
          PoolHooks.default.classification(new SQLException("x", "08006")) == Fatal,
        )
      },
    ),
    suite("failure tracking")(
      test("a fatal failure in the borrower's effect retires the connection") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(p.connection *> ZIO.fail(fatalSql)).either *> p.state
            }
          }
        } yield assertTrue(state.total == 0, state.idle == 0)
      },
      test("a recoverable failure keeps the connection") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO
                .scoped(p.connection *> ZIO.fail(new SQLException("dup", "23505")))
                .either *> p.state
            }
          }
        } yield assertTrue(state.total == 1, state.idle == 1)
      },
      test("a defect is classified the same way as a failure") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(p.connection *> ZIO.die(fatalSql)).exit *> p.state
            }
          }
        } yield assertTrue(state.total == 0)
      },
      test("turning failure tracking off pools the connection anyway") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url).copy(failureTracking = false)).flatMap { p =>
              ZIO.scoped(p.connection *> ZIO.fail(fatalSql)).either *> p.state
            }
          }
        } yield assertTrue(state.total == 1, state.idle == 1)
      },
      test("a failing JDBC call on the handle retires the connection") {
        val hooks = PoolHooks(classify = _ => Some(Fatal))
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url), hooks).flatMap { p =>
              ZIO
                .scoped(
                  p.connection.flatMap(c =>
                    ZIO.attemptBlocking(c.createStatement().execute("NOT SQL AT ALL")),
                  ),
                )
                .either *> p.state
            }
          }
        } yield assertTrue(state.total == 0)
      },
      test("a successful borrow is never retired") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c =>
                  ZIO.attemptBlocking(PoolTestSupport.queryInt(c, "SELECT 1")),
                ),
              ) *> p.state
            }
          }
        } yield assertTrue(state.total == 1, state.idle == 1)
      },
    ),
    suite("invalidate")(
      test("an invalidated connection is closed instead of pooled") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(p.connection.flatMap(p.invalidate)) *> p.state
            }
          }
        } yield assertTrue(state.total == 0, state.idle == 0)
      },
      test("invalidating twice does not drift the count") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c => p.invalidate(c) *> p.invalidate(c)),
              ) *> p.state
            }
          }
        } yield assertTrue(state.total == 0, state.idle == 0)
      },
      test("invalidating after the borrow ended leaves the pool alone") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 1)).flatMap { p =>
              for {
                borrowed <- ZIO.scoped(p.connection)
                _        <- p.invalidate(borrowed)
                _        <- ZIO.scoped(p.connection)
                state    <- p.state
              } yield state
            }
          }
        } yield assertTrue(state.total == 1, state.idle == 1)
      },
      test("a connection another layer wrapped still reaches the pool") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c => p.invalidate(c.unwrap(classOf[java.sql.Connection]))),
              ) *> p.state
            }
          }
        } yield assertTrue(state.total == 0, state.idle == 0)
      },
      test("a connection the pool never handed out is ignored") {
        for {
          url     <- backend.freshUrl
          foreign <- ZIO.attemptBlocking {
            val _ = Class.forName("org.h2.Driver")
            java.sql.DriverManager.getConnection(url)
          }
          state   <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(p.connection) *> p.invalidate(foreign) *> p.state
            }
          }
          _       <- ZIO.attemptBlocking(foreign.close())
        } yield assertTrue(state.total == 1, state.idle == 1)
      },
    ),
    suite("suspend and resume")(
      test("a suspended pool parks new borrowers until it resumes") {
        for {
          url    <- backend.freshUrl
          result <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 2)).flatMap { p =>
              for {
                _       <- p.suspend
                fiber   <- ZIO.scoped(p.connection).fork
                _       <- ZIO.sleep(50.millis)
                blocked <- fiber.poll.map(_.isEmpty)
                _       <- p.resume
                _       <- fiber.join
              } yield blocked
            }
          }
        } yield assertTrue(result)
      },
      test("a borrower already holding a connection is untouched") {
        for {
          url    <- backend.freshUrl
          answer <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(
                p.connection.flatMap(c =>
                  p.suspend *> ZIO.attemptBlocking(PoolTestSupport.queryInt(c, "SELECT 1")),
                ),
              ) <* p.resume
            }
          }
        } yield assertTrue(answer == 1)
      },
      test("suspending twice and resuming once is enough") {
        for {
          url    <- backend.freshUrl
          states <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              for {
                _         <- p.suspend *> p.suspend
                suspended <- p.state
                _         <- p.resume
                running   <- p.state
                _         <- ZIO.scoped(p.connection)
              } yield (suspended, running)
            }
          }
        } yield assertTrue(states._1.suspended, !states._2.suspended)
      },
      test("resuming a pool that was never suspended is a no-op") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap(p => p.resume *> ZIO.scoped(p.connection) *> p.state)
          }
        } yield assertTrue(!state.suspended, state.idle == 1)
      },
      test("shutdown releases a borrower parked by suspend") {
        for {
          url    <- backend.freshUrl
          result <- ZIO.scoped {
            for {
              p     <- pool(backend.config(url))
              _     <- p.suspend
              fiber <- ZIO.scoped(p.connection).either.fork
              _     <- ZIO.sleep(50.millis)
              _     <- p.shutdown
              exit  <- fiber.join
            } yield exit
          }
        } yield assert(result)(isLeft(isSubtype[PoolShutdownException](anything)))
      },
    ),
    suite("shutdown")(
      test("closes a connection a borrower is still holding") {
        for {
          url      <- backend.freshUrl
          released <- ZIO.scoped {
            for {
              p     <- pool(backend.config(url).copy(shutdownTimeout = 100.millis))
              held  <- Promise.make[Nothing, Unit]
              fiber <- ZIO
                .scoped(p.connection *> held.succeed(()) *> ZIO.never)
                .fork
              _     <- held.await
              _     <- p.shutdown
              state <- p.state
              _     <- fiber.interrupt
            } yield state
          }
        } yield assertTrue(released.shutdown, released.idle == 0)
      },
      test("shutting down twice is safe") {
        for {
          url   <- backend.freshUrl
          state <- ZIO.scoped {
            pool(backend.config(url)).flatMap { p =>
              ZIO.scoped(p.connection) *> p.shutdown *> p.shutdown *> p.state
            }
          }
        } yield assertTrue(state.total == 0, state.idle == 0, state.shutdown)
      },
      test("a connection returned after shutdown is closed, not pooled") {
        for {
          url    <- backend.freshUrl
          result <- ZIO.scoped {
            for {
              p        <- pool(backend.config(url))
              borrowed <- ZIO.scoped(p.connection.flatMap(c => p.shutdown.as(c)))
              state    <- p.state
              closed <- ZIO.attemptBlocking(borrowed.unwrap(classOf[java.sql.Connection]).isClosed)
            } yield (state, closed)
          }
        } yield assertTrue(result._1.total == 0, result._2)
      },
    ),
  ) @@ withLiveClock @@ withLiveRandom @@ timeout(120.seconds)
}
