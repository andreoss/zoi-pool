package zoi.pool.interop

import zio.jdbc.{ZConnectionPool, sqlInterpolator, transaction}
import zio.test.TestAspect._
import zio.test._
import zio.{ZIO, ZLayer, durationInt}

import zoi.pool.{ConnectionPool, H2Backend, PoolConfig}

/** zio-jdbc runs its own queries against the pool, through the adapter. */
object ZioJdbcSpec extends ZIOSpecDefault {

  private def poolLayer(url: String): ZLayer[Any, Throwable, ZConnectionPool] =
    ZioJdbc.layer(PoolConfig(url, maximumPoolSize = 4))

  def spec = suite("zio-jdbc adapter")(
    test("runs a query through zio-jdbc") {
      for {
        url    <- H2Backend.freshUrl
        answer <- transaction(sql"SELECT 1".query[Int].selectOne)
          .provide(poolLayer(url))
      } yield assertTrue(answer.contains(1))
    },
    test("creates, writes and reads a table through zio-jdbc") {
      for {
        url  <- H2Backend.freshUrl
        rows <- (for {
          _    <- transaction(
            sql"CREATE TABLE widgets (id INT PRIMARY KEY, name VARCHAR(32))".execute,
          )
          _    <- transaction(sql"INSERT INTO widgets (id, name) VALUES (1, 'nut')".insert)
          _    <- transaction(sql"INSERT INTO widgets (id, name) VALUES (2, 'bolt')".insert)
          rows <- transaction(sql"SELECT name FROM widgets ORDER BY id".query[String].selectAll)
        } yield rows).provide(poolLayer(url))
      } yield assertTrue(rows.toList == List("nut", "bolt"))
    },
    test("many zio-jdbc transactions share the pool's connections") {
      for {
        url     <- H2Backend.freshUrl
        answers <- ZIO
          .foreachPar(1 to 24)(_ => transaction(sql"SELECT 1".query[Int].selectOne))
          .provide(poolLayer(url))
      } yield assertTrue(answers.forall(_.contains(1)))
    },
    test("both views of one pool are the same pool") {
      for {
        url   <- H2Backend.freshUrl
        state <- (for {
          _     <- transaction(sql"SELECT 1".query[Int].selectOne)
          state <- ZIO.serviceWithZIO[ConnectionPool](_.state)
        } yield state)
          .provide(ZioJdbc.layers(PoolConfig(url, maximumPoolSize = 2)))
      } yield assertTrue(state.total == 1, state.idle == 1)
    },
    test("a connection zio-jdbc invalidates is dropped by the pool") {
      for {
        url <- H2Backend.freshUrl
        config = PoolConfig(url, maximumPoolSize = 2)
        state <- ZIO.scoped {
          ConnectionPool.scoped(config).flatMap { pool =>
            val adapter = ZioJdbc.asZConnectionPool(pool)
            ZIO.scoped {
              adapter.transaction.build.flatMap { env =>
                adapter.invalidate(env.get)
              }
            } *> pool.state
          }
        }
      } yield assertTrue(state.total == 0, state.idle == 0)
    },
  ) @@ withLiveClock @@ withLiveRandom @@ timeout(90.seconds)
}
