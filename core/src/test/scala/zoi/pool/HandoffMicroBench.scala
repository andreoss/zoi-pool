package zoi.pool

import zio.{Clock, Console, Ref, ZIO, ZIOAppArgs, ZIOAppDefault, durationInt}

/**
 * Compares hand-off cores directly, with tokens instead of connections, so the
 * measurement is the hand-off and nothing else.
 *
 * Run: sbt "core/Test/runMain zoi.pool.HandoffMicroBench"
 */
object HandoffMicroBench extends ZIOAppDefault {

  private val operations = 200000
  private val timeout    = 30.seconds

  override def run: ZIO[ZIOAppArgs, Any, Unit] =
    for {
      _ <- Console.printLine("core       scenario         ops/s")
      _ <- ZIO.foreachDiscard(List("lock-free", "stm")) { name =>
        ZIO.foreachDiscard(scenarios) { case (scenario, fibers, size) =>
          measure(name, scenario, fibers, size)
        }
      }
    } yield ()

  private val scenarios = List(
    ("uncontended", 1, 8),
    ("parallel-8", 8, 8),
    ("saturated-50", 50, 8),
  )

  private def core(name: String, size: Int) =
    if (name == "stm") StmHandoffCore.make[Int]("bench", size)
    else LockFreeHandoffCore.make[Int]("bench", size)

  private def measure(name: String, scenario: String, fibers: Int, size: Int): ZIO[Any, Any, Unit] =
    ZIO.scoped {
      for {
        handoff <- core(name, size)
        next    <- Ref.make(0)
        perFibre = math.max(1, operations / fibers)
        _     <- cycle(handoff, next).repeatN(999)
        start <- Clock.nanoTime
        _     <- ZIO.foreachParDiscard(1 to fibers)(_ => cycle(handoff, next).repeatN(perFibre - 1))
        end   <- Clock.nanoTime
        rate = (perFibre * fibers).toDouble * 1000000000.0 / math.max(end - start, 1L).toDouble
        _ <- Console.printLine(f"$name%-10s $scenario%-14s ${rate}%,12.0f")
      } yield ()
    }

  /** One borrow and one return, exactly as the pool drives the core. */
  private def cycle(handoff: HandoffCore[Int], next: Ref[Int]): ZIO[Any, Any, Unit] =
    handoff.acquire(timeout).flatMap {
      case HandoffCore.Acquired.Ready(token, _) => handoff.offer(token).unit
      case HandoffCore.Acquired.Reserved(_)     =>
        next.updateAndGet(_ + 1).flatMap(token => handoff.offer(token).unit)
    }

}
