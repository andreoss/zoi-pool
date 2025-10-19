package zoi.pool

import java.sql.SQLException

import zio.test.TestAspect._
import zio.test._
import zio.{Ref, Scope, ZIO, durationInt}

/**
 * Drives the boundary between the fast path and the parked path hard: far more
 * borrowers than connections, mixed with failures, invalidation, maintenance
 * and suspension, then checks the pool's books balance.
 */
object PoolSoakSpec extends ZIOSpecDefault {

  private val backend = H2Backend

  private def pool(config: PoolConfig): ZIO[Scope, Throwable, ConnectionPoolLive] =
    ConnectionPoolLive.scoped(config, PoolHooks(metrics = PoolMetrics.recording))

  private def soak(
    fibers: Int,
    rounds: Int,
    config: PoolConfig,
    action: (ConnectionPoolLive, Int) => ZIO[Any, Throwable, Unit],
  ) =
    ZIO.scoped {
      pool(config).flatMap { p =>
        for {
          failures <- Ref.make(0)
          _        <- ZIO.foreachParDiscard(1 to fibers) { fibre =>
                        ZIO.foreachDiscard(1 to rounds) { round =>
                          action(p, fibre * 31 + round).catchAll(_ => failures.update(_ + 1))
                        }
                      }
          broken   <- failures.get
          state    <- p.state
          metrics  <- p.metrics
        } yield (broken, state, metrics)
      }
    }

  private def borrow(p: ConnectionPoolLive): ZIO[Any, Throwable, Unit] =
    ZIO.scoped(
      p.connection.flatMap(c => ZIO.attemptBlocking(PoolTestSupport.queryInt(c, "SELECT 1"))),
    ).unit

  def spec = suite("soak")(
    test("many borrowers over a small pool keep the books balanced") {
      for {
        url    <- backend.freshUrl
        config  = backend.config(url).copy(maximumPoolSize = 4, connectionTimeout = 20.seconds)
        result <- soak(48, 30, config, (p, _) => borrow(p))
      } yield assertTrue(
        result._1 == 0,
        result._2.total <= 4,
        result._2.idle == result._2.total,
        result._2.active == 0,
        result._2.waiting == 0,
        result._3.acquires == 48L * 30L,
      )
    },
    test("borrowers that fail, invalidate and succeed leave nothing stranded") {
      for {
        url    <- backend.freshUrl
        config  = backend.config(url).copy(maximumPoolSize = 3, connectionTimeout = 20.seconds)
        result <- soak(
                    36,
                    24,
                    config,
                    (p, seed) =>
                      seed % 3 match {
                        case 0 => borrow(p)
                        case 1 =>
                          ZIO
                            .scoped(p.connection *> ZIO.fail(new SQLException("dead", "08006")))
                            .unit
                        case _ => ZIO.scoped(p.connection.flatMap(p.invalidate))
                      },
                  )
      } yield assertTrue(
        result._2.total <= 3,
        result._2.idle == result._2.total,
        result._2.active == 0,
        result._2.waiting == 0,
        result._3.connectionsCreated >= 1L,
      )
    },
    test("maintenance running under load never strands a connection") {
      for {
        url    <- backend.freshUrl
        config  = backend.config(url).copy(
                    maximumPoolSize = 4,
                    minimumIdle = Some(1),
                    connectionTimeout = 20.seconds,
                    maintenanceInterval = Some(5.millis),
                    idleTimeout = 20.millis,
                    keepaliveTime = 25.millis,
                    maxLifetime = 200.millis,
                  )
        result <- soak(32, 25, config, (p, _) => borrow(p))
      } yield assertTrue(
        result._1 == 0,
        result._2.total <= 4,
        result._2.idle == result._2.total,
        result._2.waiting == 0,
      )
    },
    test("suspending and resuming under load loses nothing") {
      for {
        url   <- backend.freshUrl
        config = backend.config(url).copy(maximumPoolSize = 4, connectionTimeout = 20.seconds)
        state <- ZIO.scoped {
                   pool(config).flatMap { p =>
                     for {
                       load    <- ZIO.foreachParDiscard(1 to 24)(_ =>
                                    ZIO.foreachDiscard(1 to 20)(_ => borrow(p)),
                                  ).fork
                       _       <- ZIO.foreachDiscard(1 to 6)(_ =>
                                    p.suspend *> ZIO.sleep(3.millis) *> p.resume *> ZIO.sleep(3.millis),
                                  )
                       _       <- p.resume
                       _       <- load.join
                       state   <- p.state
                     } yield state
                   }
                 }
      } yield assertTrue(
        state.total <= 4,
        state.idle == state.total,
        state.active == 0,
        state.waiting == 0,
        !state.suspended,
      )
    },
    test("a pool torn down under load closes every connection it opened") {
      for {
        url     <- backend.freshUrl
        config   = backend.config(url).copy(maximumPoolSize = 4, shutdownTimeout = 2.seconds)
        opened  <- Ref.make(List.empty[java.sql.Connection])
        _       <- ZIO.scoped {
                     pool(config).flatMap { p =>
                       ZIO.foreachParDiscard(1 to 16) { _ =>
                         ZIO
                           .scoped(
                             p.connection.flatMap(c =>
                               opened.update(c.unwrap(classOf[java.sql.Connection]) :: _),
                             ),
                           )
                           .ignore
                       }
                     }
                   }
        tracked <- opened.get
        closed  <- ZIO.foreach(tracked.distinct)(c => ZIO.attemptBlocking(c.isClosed))
      } yield assertTrue(closed.nonEmpty, closed.forall(isShut => isShut))
    },
  ) @@ withLiveClock @@ withLiveRandom @@ sequential @@ timeout(180.seconds)
}
