package zoi.pool

import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Promise, Ref, ZIO, durationInt}

import zoi.pool.PoolTestSupport._

/**
 * The behaviour every pool must show on every database. A backend is covered
 * when it passes this suite; per-database specifics are added on top of it.
 */
object ConnectionPoolContract {

  def tests(backend: JdbcBackend): Spec[Any, Throwable] =
    suite(s"ConnectionPool contract: ${backend.name}")(
      test("borrows a connection that works") {
        for {
          url    <- backend.freshUrl
          answer <- withPool(backend.config(url))(pool =>
            borrow(pool)(queryInt(_, backend.selectOne)),
          )
        } yield assertTrue(answer == 1)
      },
      test("returns the connection to the pool when the borrower's scope closes") {
        for {
          url   <- backend.freshUrl
          state <- withPool(backend.config(url)) { pool =>
            borrow(pool)(_.isClosed) *> pool.state
          }
        } yield assertTrue(state.idle == 1, state.active == 0, state.total == 1)
      },
      test("reuses the physical connection instead of opening another") {
        for {
          url    <- backend.freshUrl
          result <- withPool(backend.config(url)) { pool =>
            for {
              first  <- borrow(pool)(_.unwrap(classOf[java.sql.Connection]))
              second <- borrow(pool)(_.unwrap(classOf[java.sql.Connection]))
              state  <- pool.state
            } yield (first eq second) && (state.total == 1)
          }
        } yield assertTrue(result)
      },
      test("hands a returned connection to a waiting borrower") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = 1)
          answer <- withPool(config) { pool =>
            for {
              held    <- Promise.make[Nothing, Unit]
              holder  <- ZIO
                .scoped(pool.connection *> held.succeed(()) *> ZIO.sleep(50.millis))
                .fork
              _       <- held.await
              waiting <- borrow(pool)(queryInt(_, backend.selectOne))
              _       <- holder.join
            } yield waiting
          }
        } yield assertTrue(answer == 1)
      },
      test("never opens more connections than maximumPoolSize") {
        val cap = 3
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = cap)
          highest <- withPool(config) { pool =>
            for {
              peak    <- Ref.make(0)
              sampler <- pool.state
                .flatMap(s => peak.update(_ max s.total))
                .repeat(zio.Schedule.spaced(1.millis))
                .fork
              _ <- ZIO.foreachParDiscard(1 to 24)(_ => borrow(pool)(queryInt(_, backend.selectOne)))
              _ <- sampler.interrupt
              state    <- pool.state
              observed <- peak.get
            } yield observed max state.total
          }
        } yield assertTrue(highest <= cap, highest > 0)
      },
      test("fails with a timeout once the pool is exhausted") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = 1, connectionTimeout = 60.millis)
          result <- withPool(config) { pool =>
            ZIO.scoped(
              pool.connection *> ZIO.scoped(pool.connection).either,
            )
          }
        } yield assert(result)(isLeft(isSubtype[PoolTimeoutException](anything)))
      },
      test("a failed borrow still returns the connection") {
        for {
          url   <- backend.freshUrl
          state <- withPool(backend.config(url)) { pool =>
            ZIO.scoped(pool.connection *> ZIO.fail(new RuntimeException("boom"))).either *>
              pool.state
          }
        } yield assertTrue(state.idle == 1, state.active == 0)
      },
      test("an interrupted borrower does not strand its connection") {
        for {
          url   <- backend.freshUrl
          state <- withPool(backend.config(url).copy(maximumPoolSize = 1)) { pool =>
            for {
              entered <- Promise.make[Nothing, Unit]
              fiber   <- ZIO
                .scoped(pool.connection *> entered.succeed(()) *> ZIO.never)
                .fork
              _       <- entered.await
              _       <- fiber.interrupt
              state   <- pool.state
            } yield state
          }
        } yield assertTrue(state.idle == 1, state.active == 0, state.total == 1)
      },
      test("closes every connection it owns when its scope closes") {
        for {
          url        <- backend.freshUrl
          connection <- ZIO.scoped {
            ConnectionPool
              .scoped(backend.config(url))
              .flatMap(pool => ZIO.scoped(pool.connection))
          }
          closed     <- ZIO.attemptBlocking(connection.isClosed)
        } yield assertTrue(closed)
      },
      test("refuses to hand out connections after shutdown") {
        for {
          url    <- backend.freshUrl
          pool   <- ZIO.scoped(ConnectionPool.scoped(backend.config(url)))
          result <- ZIO.scoped(pool.connection).either
          state  <- pool.state
        } yield assert(result)(isLeft(isSubtype[PoolShutdownException](anything))) &&
          assertTrue(state.shutdown, state.total == 0, state.idle == 0)
      },
      test("reports what it is holding") {
        for {
          url   <- backend.freshUrl
          state <- withPool(backend.config(url).copy(maximumPoolSize = 2)) { pool =>
            ZIO.scoped(pool.connection *> pool.state)
          }
        } yield assertTrue(state.active == 1, state.idle == 0, state.total == 1, state.waiting == 0)
      },
      test("serves many borrowers through a single connection") {
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(maximumPoolSize = 1)
          answers <- withPool(config) { pool =>
            ZIO.foreachPar(1 to 16)(_ => borrow(pool)(queryInt(_, backend.selectOne)))
          }
        } yield assertTrue(answers.forall(_ == 1), answers.length == 16)
      },
      test("hands out connections through a plain DataSource") {
        for {
          url    <- backend.freshUrl
          answer <- withPool(backend.config(url)) { pool =>
            ZIO.attemptBlocking {
              val source     = pool.dataSource
              val connection = source.getConnection()
              try queryInt(connection, backend.selectOne)
              finally connection.close()
            }
          }
        } yield assertTrue(answer == 1)
      },
      test("a DataSource connection goes back to the pool when it is closed") {
        for {
          url   <- backend.freshUrl
          state <- withPool(backend.config(url)) { pool =>
            ZIO.attemptBlocking {
              val connection = pool.dataSource.getConnection()
              connection.close()
            } *> pool.state
          }
        } yield assertTrue(state.idle == 1, state.active == 0, state.total == 1)
      },
      test("closing a borrowed connection twice returns it once") {
        for {
          url   <- backend.freshUrl
          state <- withPool(backend.config(url)) { pool =>
            ZIO.attemptBlocking {
              val connection = pool.dataSource.getConnection()
              connection.close()
              connection.close()
            } *> pool.state
          }
        } yield assertTrue(state.idle == 1, state.total == 1)
      },
      test("a returned connection rejects further use") {
        for {
          url     <- backend.freshUrl
          outcome <- withPool(backend.config(url)) { pool =>
            ZIO.attemptBlocking {
              val connection = pool.dataSource.getConnection()
              connection.close()
              (connection.isClosed, scala.util.Try(connection.createStatement()).isFailure)
            }
          }
        } yield assertTrue(outcome._1, outcome._2)
      },
      test("statements a borrower left open are closed when it returns") {
        for {
          url       <- backend.freshUrl
          statement <- withPool(backend.config(url)) { pool =>
            ZIO.attemptBlocking {
              val connection = pool.dataSource.getConnection()
              val opened     = connection.createStatement()
              connection.close()
              opened
            }
          }
          closed    <- ZIO.attemptBlocking(statement.isClosed)
        } yield assertTrue(closed)
      },
      test("a borrower that closes its own connection does not break the scope") {
        for {
          url   <- backend.freshUrl
          state <- withPool(backend.config(url)) { pool =>
            ZIO.scoped(pool.connection.flatMap(c => ZIO.attemptBlocking(c.close()))) *>
              pool.state
          }
        } yield assertTrue(state.idle == 1, state.total == 1)
      },
      test("the DataSource refuses per-call credentials") {
        for {
          url     <- backend.freshUrl
          outcome <- withPool(backend.config(url)) { pool =>
            ZIO.attemptBlocking(pool.dataSource.getConnection("a", "b")).either
          }
        } yield assert(outcome)(
          isLeft(isSubtype[java.sql.SQLFeatureNotSupportedException](anything)),
        )
      },
    ) @@ withLiveClock @@ withLiveRandom @@ timeout(5.minutes)
}

/** Runs the contract suite against one backend. */
abstract class ConnectionPoolContractSpec extends ZIOSpecDefault {
  def backend: JdbcBackend
  override def spec: Spec[Any, Throwable] = ConnectionPoolContract.tests(backend)
}
