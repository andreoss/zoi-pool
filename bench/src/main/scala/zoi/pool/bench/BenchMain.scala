package zoi.pool.bench

import zio.{Clock, Console, ExitCode, Ref, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault}

/**
 * Runs one workload set through every selected pool over one JDBC URL, so
 * every number in the report comes from the same run on the same database.
 */
object BenchMain extends ZIOAppDefault {

  override def run: ZIO[ZIOAppArgs with Scope, Any, ExitCode] =
    for {
      args     <- ZIOAppArgs.getArgs
      settings  = BenchSettings.load(args.toList)
      _        <- announce(settings)
      results  <- measureAll(settings)
      _        <- Console.printLine(Report.render(settings, results))
      failures  = Report.regressions(settings, results)
      _        <- ZIO.foreachDiscard(failures)(line => Console.printLineError(s"REGRESSION $line"))
    } yield if (failures.isEmpty) ExitCode.success else ExitCode.failure

  private def announce(settings: BenchSettings): Task[Unit] =
    ZIO.when(settings.output != "csv") {
      Console.printLine(
        s"database=${settings.database} pools=${settings.pools.mkString(",")} " +
          s"mode=${settings.mode} iterations=${settings.iterations} " +
          s"warmup=${settings.warmup} rounds=${settings.rounds} forks=${settings.forks}",
      )
    }.unit

  /**
   * Forks are interleaved across pools rather than run pool by pool, so a noisy
   * moment on the machine lands on every pool instead of penalising whichever
   * one happened to be measured at the time.
   */
  private def measureAll(settings: BenchSettings): Task[List[Measurement]] = {
    val pools     = settings.pools.flatMap(BenchPool.byName)
    val workloads = Workload.selected(settings)
    ZIO
      .foreach(workloads) { workload =>
        ZIO
          .foreach(1 to settings.forks) { _ =>
            ZIO.foreach(pools)(pool => sample(settings, pool, workload).map(pool.name -> _))
          }
          .map(aggregate(settings, workload, pools, _))
      }
      .map(_.flatten)
  }

  private def aggregate(
    settings: BenchSettings,
    workload: Workload,
    pools: List[BenchPool],
    forks: Seq[List[(String, Sample)]],
  ): List[Measurement] =
    pools.map { pool =>
      val samples = forks.flatten.collect { case (name, sample) if name == pool.name => sample }
      if (settings.mode == "latency")
        Measurement.latency(
          settings.database,
          workload.name,
          pool.name,
          samples.flatMap(_.nanos).toArray,
        )
      else
        Measurement.throughput(
          settings.database,
          workload.name,
          pool.name,
          samples.map(_.opsPerSecond).toList,
        )
    }

  private def sample(
    settings: BenchSettings,
    pool: BenchPool,
    workload: Workload,
  ): Task[Sample] =
    ZIO.scoped {
      pool.open(settings).flatMap { borrow =>
        Workload.prepare(borrow) *>
          Workload.repeat(workload, borrow, settings.warmup) *>
          (if (settings.mode == "latency") latency(settings, workload, borrow)
           else throughput(settings, workload, borrow))
      }
    }

  private def throughput(
    settings: BenchSettings,
    workload: Workload,
    borrow: BenchPool.Borrow,
  ): Task[Sample] =
    ZIO
      .foreach(1 to settings.rounds)(_ => timed(workload, borrow, settings.iterations))
      .map(rounds => Sample(rounds.max, Nil))

  private def timed(workload: Workload, borrow: BenchPool.Borrow, count: Int): Task[Double] =
    for {
      start <- Clock.nanoTime
      _     <- Workload.repeat(workload, borrow, count)
      end   <- Clock.nanoTime
      spent  = math.max(end - start, 1L)
    } yield count.toDouble * 1000000000.0 / spent.toDouble

  private def latency(
    settings: BenchSettings,
    workload: Workload,
    borrow: BenchPool.Borrow,
  ): Task[Sample] =
    for {
      samples  <- Ref.make(List.empty[Long])
      perFibre  = math.max(1, settings.iterations / workload.fibers)
      _        <- ZIO.foreachParDiscard(1 to workload.fibers) { _ =>
                    ZIO.foreachDiscard(1 to perFibre) { _ =>
                      for {
                        start <- Clock.nanoTime
                        _     <- workload.run(borrow)
                        end   <- Clock.nanoTime
                        _     <- samples.update((end - start) :: _)
                      } yield ()
                    }
                  }
      taken    <- samples.get
