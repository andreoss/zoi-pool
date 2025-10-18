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

  private def measureAll(settings: BenchSettings): Task[List[Measurement]] = {
    val pools     = settings.pools.flatMap(BenchPool.byName)
    val workloads = Workload.selected(settings)
    ZIO
      .foreach(workloads) { workload =>
        ZIO.foreach(pools)(pool => measure(settings, pool, workload))
      }
      .map(_.flatten)
  }

  private def measure(
    settings: BenchSettings,
    pool: BenchPool,
    workload: Workload,
  ): Task[Measurement] =
    ZIO.scoped {
      pool.open(settings).flatMap { borrow =>
        Workload.prepare(borrow) *>
          Workload.repeat(workload, borrow, settings.warmup) *>
          (if (settings.mode == "latency") latency(settings, pool, workload, borrow)
           else throughput(settings, pool, workload, borrow))
      }
    }

  private def throughput(
    settings: BenchSettings,
    pool: BenchPool,
    workload: Workload,
    borrow: BenchPool.Borrow,
  ): Task[Measurement] =
    ZIO
      .foreach(1 to settings.forks) { _ =>
        ZIO
          .foreach(1 to settings.rounds)(_ => timed(workload, borrow, settings.iterations))
          .map(_.max)
      }
      .map(samples =>
        Measurement.throughput(settings.database, workload.name, pool.name, samples.toList),
      )

  private def timed(workload: Workload, borrow: BenchPool.Borrow, count: Int): Task[Double] =
    for {
      start <- Clock.nanoTime
      _     <- Workload.repeat(workload, borrow, count)
      end   <- Clock.nanoTime
      spent  = math.max(end - start, 1L)
    } yield count.toDouble * 1000000000.0 / spent.toDouble

  private def latency(
    settings: BenchSettings,
    pool: BenchPool,
    workload: Workload,
    borrow: BenchPool.Borrow,
  ): Task[Measurement] =
    for {
      samples <- Ref.make(List.empty[Long])
      _       <- ZIO.foreachDiscard(1 to settings.iterations) { _ =>
                   for {
                     start <- Clock.nanoTime
                     _     <- workload.run(borrow)
                     end   <- Clock.nanoTime
                     _     <- samples.update((end - start) :: _)
                   } yield ()
                 }
      taken   <- samples.get
    } yield Measurement.latency(settings.database, workload.name, pool.name, taken.toArray)
}
