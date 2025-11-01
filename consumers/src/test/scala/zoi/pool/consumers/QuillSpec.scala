package zoi.pool.consumers

import javax.sql.DataSource

import io.getquill._
import io.getquill.jdbczio.Quill
import zio.test.TestAspect._
import zio.test._
import zio.{ZIO, ZLayer, durationInt}
import zoi.pool.{ConnectionPool, H2Backend, PoolConfig, PoolTestSupport}

final case class Widget(id: Int, name: String)

/**
 * The migration this library exists for: zio-quill takes a `DataSource` from
 * the environment, so swapping the pool is swapping that one layer.
 */
object QuillSpec extends ZIOSpecDefault {

  private def dataSource(url: String): ZLayer[Any, Throwable, DataSource] =
    ConnectionPool.dataSourceLayer(PoolConfig(url, maximumPoolSize = 4, poolName = "quill"))

  private def createTable(url: String) =
    ZIO.attemptBlocking {
      val _          = Class.forName("org.h2.Driver")
      val connection = java.sql.DriverManager.getConnection(url)
      try {
        val statement = connection.createStatement()
        try {
          statement.execute("CREATE TABLE IF NOT EXISTS Widget (id INT PRIMARY KEY, name VARCHAR(32))")
          statement.execute("DELETE FROM Widget")
          statement.execute("INSERT INTO Widget (id, name) VALUES (1, 'nut'), (2, 'bolt')")
        } finally statement.close()
      } finally connection.close()
    }

  def spec = suite("zio-quill")(
    test("a Quill context reads through the pool") {
      for {
        url  <- H2Backend.freshUrl
        _    <- createTable(url)
        rows <- ZIO
                  .serviceWithZIO[Quill.H2[Literal]] { ctx =>
                    import ctx._
                    ctx.run(quote(query[Widget]))
                  }
                  .provide(dataSource(url), Quill.H2.fromNamingStrategy(Literal))
      } yield assertTrue(rows.map(_.name).sorted == List("bolt", "nut"))
    },
    test("a Quill transaction runs on one pooled connection") {
      for {
        url    <- H2Backend.freshUrl
        _      <- createTable(url)
        result <- ZIO.scoped {
                    ConnectionPool.scoped(PoolConfig(url, maximumPoolSize = 2)).flatMap { pool =>
                      val ctx = Quill.H2(Literal, pool.dataSource)
                      import ctx._
                      ctx.transaction(ctx.run(quote(query[Widget].size))) <*> pool.state
                    }
                  }
      } yield assertTrue(result._1 == 2L, result._2.active == 0)
    },
    test("many Quill queries share the pool and give it back") {
      for {
        url    <- H2Backend.freshUrl
        _      <- createTable(url)
        result <- ZIO
                    .serviceWithZIO[Quill.H2[Literal]] { ctx =>
                      import ctx._
                      ZIO.foreachPar(1 to 24)(_ => ctx.run(quote(query[Widget].size)))
                    }
                    .provide(dataSource(url), Quill.H2.fromNamingStrategy(Literal))
      } yield assertTrue(result.forall(_ == 2L), result.length == 24)
    },
    test("the pool is left clean after Quill is done with it") {
      for {
        url   <- H2Backend.freshUrl
        _     <- createTable(url)
        state <- ZIO.scoped {
                   ConnectionPool.scoped(PoolConfig(url, maximumPoolSize = 4)).flatMap { pool =>
                     val ctx = Quill.H2(Literal, pool.dataSource)
                     import ctx._
                     ctx.run(quote(query[Widget])) *> pool.state
                   }
                 }
      } yield assertTrue(state.active == 0, state.idle == state.total, state.total >= 1)
    },
    test("a plain JDBC caller and Quill can share one pool") {
      for {
        url    <- H2Backend.freshUrl
        _      <- createTable(url)
        result <- ZIO.scoped {
                    ConnectionPool.scoped(PoolConfig(url, maximumPoolSize = 2)).flatMap { pool =>
                      val ctx = Quill.H2(Literal, pool.dataSource)
                      import ctx._
                      for {
                        viaQuill <- ctx.run(quote(query[Widget].size))
                        viaJdbc  <- ZIO.attemptBlocking {
                                      val connection = pool.dataSource.getConnection()
                                      try PoolTestSupport.queryInt(connection, "SELECT COUNT(*) FROM Widget")
                                      finally connection.close()
                                    }
                      } yield (viaQuill, viaJdbc)
                    }
                  }
      } yield assertTrue(result._1 == 2L, result._2 == 2)
    },
  ) @@ withLiveClock @@ withLiveRandom @@ timeout(120.seconds)
}
