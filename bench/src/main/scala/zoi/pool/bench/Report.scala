package zoi.pool.bench

import java.util.Locale

import scala.io.Source

/** Renders results, and compares them against a stored baseline. */
object Report {

  def render(settings: BenchSettings, results: List[Measurement]): String =
    if (settings.output == "csv") csv(settings, results)
    else if (settings.mode == "latency") latencyTable(results)
    else throughputTable(results)

  private def throughputTable(results: List[Measurement]): String = {
    val pools  = results.map(_.pool).distinct
    val header = ("workload" +: pools.map(p => s"$p ops/s") :+ "vs hikari").map(pad)
    val rows   = results.map(_.workload).distinct.map { workload =>
      val row = results.filter(_.workload == workload)
      val by  = pools.map(pool => row.find(_.pool == pool).map(_.opsPerSecond).getOrElse(0.0))
      (workload +: by.map(number) :+ ratio(row)).map(pad)
    }
    (header :: rows).map(_.mkString(" | ")).mkString("\n")
  }

  private def latencyTable(results: List[Measurement]): String = {
    val header = List("workload", "pool", "p50 us", "p99 us", "p999 us").map(pad)
    val rows   = results.map { result =>
      List(result.workload, result.pool, number(result.p50Micros), number(result.p99Micros), number(result.p999Micros))
        .map(pad)
    }
    (header :: rows).map(_.mkString(" | ")).mkString("\n")
  }

  private def csv(settings: BenchSettings, results: List[Measurement]): String = {
    val header = "database,workload,pool,ops_per_second,lowest,highest,p50_micros,p99_micros,p999_micros"
    val rows   = results.map { r =>
      List(
        settings.database,
        r.workload,
        r.pool,
        format(r.opsPerSecond),
        format(r.lowest),
        format(r.highest),
        format(r.p50Micros),
        format(r.p99Micros),
        format(r.p999Micros),
      ).mkString(",")
    }
    (header :: rows).mkString("\n")
  }

  /**
   * A run regresses when the library's gap to the incumbent grew past the
   * tolerance and the library's own rate fell by as much. A faster incumbent
   * widens the gap on its own, and that is not the library regressing.
   */
  def regressions(
    settings: BenchSettings,
    results: List[Measurement],
  ): List[String] =
    settings.baseline.toList.flatMap { path =>
      val stored = read(path)
      val rates  = library(results)
      ratios(results).flatMap { case (key, current) =>
        stored.ratios.get(key).toList.flatMap { previous =>
          val wider  = (current - previous) / math.max(previous, 1e-9)
          val slower = stored.rates
            .get(key)
            .map(was => (was - rates.getOrElse(key, 0.0)) / math.max(was, 1e-9))
            .getOrElse(0.0)
          if (wider > settings.tolerance && slower > settings.tolerance)
            List(
              f"$key%s: gap to hikari grew ${previous}%.2fx -> ${current}%.2fx " +
                f"while its own rate fell ${slower * 100}%.0f%%",
            )
          else Nil
        }
      }
    }

  private def ratios(results: List[Measurement]): Map[String, Double] =
    results
      .groupBy(_.workload)
      .flatMap { case (workload, row) =>
        row.find(_.pool == "hikari").map(_.opsPerSecond).filter(_ > 0.0).toList.flatMap { reference =>
          row.filter(_.pool == "zoi").filter(_.opsPerSecond > 0.0).map { measurement =>
            s"$workload/${measurement.pool}" -> reference / measurement.opsPerSecond
          }
        }
      }

  private final case class Baseline(ratios: Map[String, Double], rates: Map[String, Double])

  private def library(results: List[Measurement]): Map[String, Double] =
    results.filter(_.pool == "zoi").map(m => s"${m.workload}/${m.pool}" -> m.opsPerSecond).toMap

  private def read(path: String): Baseline = {
    val file = new java.io.File(path)
    if (!file.isFile) throw new IllegalArgumentException(s"no baseline at $path")
    val source = Source.fromFile(path)
    try {
      val rows = source.getLines().drop(1).toList.map(_.split(",").toList)
      val parsed = rows.collect {
        case _ :: workload :: pool :: ops :: _ => (workload, pool, ops.toDouble)
      }
      val stored = parsed.map { case (workload, pool, ops) =>
        Measurement("", workload, pool, ops, ops, ops, 0.0, 0.0, 0.0)
      }
      Baseline(ratios(stored), library(stored))
    } finally source.close()
  }

  private def ratio(row: List[Measurement]): String = {
    val reference = row.find(_.pool == "hikari").map(_.opsPerSecond).getOrElse(0.0)
    val measured  = row.find(_.pool == "zoi").map(_.opsPerSecond).getOrElse(0.0)
    if (reference <= 0.0 || measured <= 0.0) "-" else f"${reference / measured}%.2fx"
  }

  private def number(value: Double): String = f"$value%,.0f".replace(',', ' ')

  private def format(value: Double): String = String.format(Locale.ROOT, "%.3f", Double.box(value))

  private def pad(value: String): String = value.padTo(14, ' ')
}
