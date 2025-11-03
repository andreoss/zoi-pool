package zoi.pool.consumers

import scala.concurrent.ExecutionContext

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie._
import doobie.implicits._
import zio.test.TestAspect._
import zio.test._
import zio.{ZIO, durationInt}
import zoi.pool.{ConnectionPool, H2Backend, PoolConfig}

/**
 * doobie is a Cats Effect consumer with no ZIO of its own, which is what makes
 * it the proof that the `DataSource` surface carries no framework with it.
 */
object DoobieSpec extends ZIOSpecDefault {

  private def withTransactor[A](url: String)(use: Transactor[IO] => A) =
    ZIO.scoped {
      ConnectionPool.scoped(PoolConfig(url, maximumPoolSize = 4, poolName = "doobie")).flatMap {
        pool =>
          ZIO.attemptBlocking(
            use(Transactor.fromDataSource[IO](pool.dataSource, ExecutionContext.global)),
          )
      }
    }

  def spec = suite("doobie")(
    test("a transactor reads through the pool") {
      for {
        url    <- H2Backend.freshUrl
        answer <- withTransactor(url)(xa =>
          sql"SELECT 1".query[Int].unique.transact(xa).unsafeRunSync(),
        )
      } yield assertTrue(answer == 1)
    },
    test("a transactor writes and reads back through the pool") {
      for {
        url    <- H2Backend.freshUrl
        result <- withTransactor(url) { xa =>
          val program = for {
            _    <- sql"CREATE TABLE gadget (id INT PRIMARY KEY, name VARCHAR(32))".update.run
            _    <- sql"INSERT INTO gadget (id, name) VALUES (1, 'cog')".update.run
            name <- sql"SELECT name FROM gadget WHERE id = 1".query[String].unique
          } yield name
          program.transact(xa).unsafeRunSync()
        }
      } yield assertTrue(result == "cog")
    },
    test("many doobie transactions share the pool") {
      for {
        url     <- H2Backend.freshUrl
        answers <- withTransactor(url) { xa =>
          import cats.implicits._
          List.fill(24)(sql"SELECT 1".query[Int].unique.transact(xa)).parSequence.unsafeRunSync()
        }
      } yield assertTrue(answers.forall(_ == 1), answers.length == 24)
    },
    test("the pool is left clean after doobie is done") {
      for {
        url   <- H2Backend.freshUrl
        state <- ZIO.scoped {
          ConnectionPool.scoped(PoolConfig(url, maximumPoolSize = 2)).flatMap { pool =>
            ZIO.attemptBlocking {
              val xa = Transactor.fromDataSource[IO](pool.dataSource, ExecutionContext.global)
              sql"SELECT 1".query[Int].unique.transact(xa).unsafeRunSync()
            } *> pool.state
          }
        }
      } yield assertTrue(state.active == 0, state.idle == state.total, state.total >= 1)
    },
    test("a failed doobie transaction rolls back and returns the connection") {
      for {
        url    <- H2Backend.freshUrl
        result <- ZIO.scoped {
          ConnectionPool.scoped(PoolConfig(url, maximumPoolSize = 2)).flatMap { pool =>
            ZIO.attemptBlocking {
              val xa      = Transactor.fromDataSource[IO](pool.dataSource, ExecutionContext.global)
              val program = for {
                _ <- sql"CREATE TABLE doohickey (id INT PRIMARY KEY)".update.run
                _ <- sql"INSERT INTO doohickey (id) VALUES (1)".update.run
                _ <- sql"INSERT INTO doohickey (id) VALUES (1)".update.run
              } yield ()
              program.transact(xa).attempt.unsafeRunSync()
              sql"SELECT COUNT(*) FROM doohickey".query[Int].unique.transact(xa).unsafeRunSync()
            } <*> pool.state
          }
        }
      } yield assertTrue(result._1 == 0, result._2.active == 0)
    },
  ) @@ withLiveClock @@ withLiveRandom @@ timeout(120.seconds)
}
