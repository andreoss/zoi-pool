package zoi.pool.examples

import zio.{Console, ZIO, ZIOAppDefault}

import zoi.pool.{ConnectionPool, PoolConfig}

/** Borrow a connection, run a query, give it back. */
object BasicUsage extends ZIOAppDefault {

  private val config = PoolConfig(
    url = "jdbc:h2:mem:basic;DB_CLOSE_DELAY=-1",
    maximumPoolSize = 5,
  )

  override def run =
    ZIO.scoped {
      for {
        pool   <- ConnectionPool.scoped(config)
        answer <- ZIO.scoped {
          pool.connection.flatMap { connection =>
            ZIO.attemptBlocking {
              val statement = connection.createStatement()
              try {
                val results = statement.executeQuery("SELECT 1")
                try {
                  results.next()
                  results.getInt(1)
                } finally results.close()
              } finally statement.close()
            }
          }
        }
        _      <- Console.printLine(s"the database says $answer")
        state  <- pool.state
        _      <- Console.printLine(s"pool holds ${state.total}, ${state.idle} of them idle")
      } yield ()
    }
}
