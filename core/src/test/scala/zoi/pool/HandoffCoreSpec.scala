package zoi.pool

import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{Chunk, Promise, Ref, Scope, UIO, ZIO, durationInt}

import HandoffCore.{Acquired, Offered}

/**
 * The hand-off contract, run against both cores: the one the pool ships and
 * the transactional one it is modelled on. A difference between them is a bug
 * in the fast one, which is the point of keeping the slow one.
 */
object HandoffCoreSpec extends ZIOSpecDefault {

  private val implementations: List[(String, (String, Int) => ZIO[Scope, Nothing, HandoffCore[Int]])] =
    List(
      "lock-free" -> ((name, size) => LockFreeHandoffCore.make[Int](name, size)),
      "stm"       -> ((name, size) => StmHandoffCore.make[Int](name, size)),
    )

  private def awaitWaiting(core: HandoffCore[Int], count: Int): UIO[Int] =
    (ZIO.sleep(2.millis) *> core.waitingCount).repeatUntil(_ >= count)

  private def take(core: HandoffCore[Int], token: Int): UIO[Int] =
    core.acquire(5.seconds).orDie.flatMap {
      case Acquired.Ready(value, _) => ZIO.succeed(value)
      case Acquired.Reserved(_)     => ZIO.succeed(token)
    }

  def spec = suite("hand-off core")(
    implementations.map { case (name, make) =>
      suite(name)(
        test("an empty core reserves a slot rather than waiting") {
          ZIO.scoped {
            for {
              core     <- make("t", 2)
              acquired <- core.acquire(1.second)
              total    <- core.totalCount
            } yield assertTrue(acquired == Acquired.Reserved(waited = false), total == 1)
          }
        },
        test("a returned resource is handed back out") {
          ZIO.scoped {
            for {
              core     <- make("t", 2)
              _        <- core.acquire(1.second)
              _        <- core.offer(7)
              acquired <- core.acquire(1.second)
            } yield assertTrue(acquired == Acquired.Ready(7, waited = false))
          }
        },
        test("the cap is never exceeded, however many acquirers arrive at once") {
          ZIO.scoped {
            for {
              core   <- make("t", 3)
              peak   <- Ref.make(0)
              _      <- ZIO.foreachParDiscard(1 to 40) { index =>
                          take(core, index).flatMap { token =>
                            core.totalCount.flatMap(t => peak.update(_ max t)) *>
                              core.offer(token)
                          }
                        }
              worst  <- peak.get
              total  <- core.totalCount
            } yield assertTrue(worst <= 3, total <= 3, total > 0)
          }
        },
        test("a parked acquirer is woken by a return") {
          ZIO.scoped {
            for {
              core    <- make("t", 1)
              _       <- core.acquire(5.seconds)
              parked  <- core.acquire(5.seconds).fork
              waiting <- awaitWaiting(core, 1)

              _       <- core.offer(9)
              got     <- parked.join
            } yield assertTrue(waiting == 1, got == Acquired.Ready(9, waited = true))
          }
        },
        test("a parked acquirer is woken when a slot is freed") {
          ZIO.scoped {
            for {
              core   <- make("t", 1)
              _      <- core.acquire(5.seconds)
              parked <- core.acquire(5.seconds).fork
              _      <- awaitWaiting(core, 1)
              _      <- core.releaseSlot
              got    <- parked.join
            } yield assertTrue(got == Acquired.Reserved(waited = true))
          }
        },
        test("a parked acquirer times out and leaves nothing behind") {
          ZIO.scoped {
            for {
              core    <- make("t", 1)
              _       <- core.acquire(5.seconds)
              outcome <- core.acquire(120.millis).either
              waiting <- core.waitingCount
              total   <- core.totalCount
            } yield assert(outcome)(isLeft(isSubtype[PoolTimeoutException](anything))) &&
              assertTrue(waiting == 0, total == 1)
          }
        },
        test("an interrupted acquirer gives back what it was handed") {
          ZIO.scoped {
            for {
              core    <- make("t", 1)
              _       <- core.acquire(5.seconds)
              entered <- Promise.make[Nothing, Unit]
              parked  <- (entered.succeed(()) *> core.acquire(5.seconds)).fork
              _       <- entered.await *> awaitWaiting(core, 1)
              _       <- parked.interrupt
              _       <- core.offer(4)
              _       <- ZIO.sleep(40.millis)
              waiting <- core.waitingCount
              idle    <- core.idleCount
            } yield assertTrue(idle == 1, waiting == 0)
          }
        },
        test("shutdown wakes every parked acquirer with a failure") {
          ZIO.scoped {
            for {
              core    <- make("t", 1)
              _       <- core.acquire(5.seconds)
              parked  <- ZIO.foreachPar(1 to 4)(_ => core.acquire(5.seconds).either).fork
              _       <- awaitWaiting(core, 4)
              _       <- core.shutdown
              results <- parked.join
            } yield assertTrue(results.forall(_.isLeft))
          }
        },
        test("a shut down core discards what is returned to it") {
          ZIO.scoped {
            for {
              core     <- make("t", 2)
              _        <- core.shutdown
              outcome  <- core.offer(1)
              acquired <- core.acquire(200.millis).either
            } yield assertTrue(outcome == Offered.Discarded, acquired.isLeft)
          }
        },
        test("idle resources are drained oldest first and by predicate") {
          ZIO.scoped {
            for {
              core    <- make("t", 4)
              _       <- ZIO.foreachDiscard(1 to 4)(_ => core.acquire(1.second))
              _       <- ZIO.foreachDiscard(List(1, 2, 3, 4))(core.offer)
              chosen  <- core.takeIdleWhere(2, _ % 2 == 1)
              left    <- core.idleCount
              drained <- core.drainIdle
            } yield assertTrue(
              chosen == Chunk(1, 3),
              left == 2,
              drained.toSet == Set(2, 4),
            )
          }
        },
        test("removing a specific idle resource reports whether it was there") {
          ZIO.scoped {
            for {
              core   <- make("t", 2)
              _      <- core.acquire(1.second)
              _      <- core.offer(5)
              first  <- core.removeIdle(5)
              second <- core.removeIdle(5)
            } yield assertTrue(first, !second)
          }
        },
        test("many acquirers over a small core keep the cap and lose nothing") {
          ZIO.scoped {
            for {
              core  <- make("t", 4)
              seen  <- Ref.make(Set.empty[Int])
              _     <- ZIO.foreachParDiscard(1 to 200) { index =>
                         take(core, 1000 + index).flatMap { token =>
                           seen.update(_ + token) *> core.offer(token)
                         }
                       }
              total <- core.totalCount
              idle  <- core.idleCount
            } yield assertTrue(total <= 4, total >= 1, idle == total)
          }
        },
      )
    }: _*,
  ) @@ withLiveClock @@ timeout(120.seconds)
}
