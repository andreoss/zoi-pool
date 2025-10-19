package zoi.pool.bench

/** What one pool scored on one workload. */
final case class Measurement(
  database: String,
  workload: String,
  pool: String,
  opsPerSecond: Double,
  lowest: Double,
  highest: Double,
  p50Micros: Double,
  p99Micros: Double,
  p999Micros: Double,
)

object Measurement {

  def throughput(
    database: String,
    workload: String,
    pool: String,
    samples: List[Double],
  ): Measurement =
    Measurement(
      database = database,
      workload = workload,
      pool = pool,
      opsPerSecond = median(samples),
      lowest = samples.min,
      highest = samples.max,
      p50Micros = 0.0,
      p99Micros = 0.0,
      p999Micros = 0.0,
    )

  def latency(
    database: String,
    workload: String,
    pool: String,
    nanos: Array[Long],
  ): Measurement = {
    java.util.Arrays.sort(nanos)
    Measurement(
      database = database,
      workload = workload,
      pool = pool,
      opsPerSecond = 0.0,
      lowest = 0.0,
      highest = 0.0,
      p50Micros = percentile(nanos, 0.50),
      p99Micros = percentile(nanos, 0.99),
      p999Micros = percentile(nanos, 0.999),
    )
  }

  def median(values: List[Double]): Double = {
    val sorted = values.sorted
    if (sorted.isEmpty) 0.0
    else if (sorted.length % 2 == 1) sorted(sorted.length / 2)
    else (sorted(sorted.length / 2 - 1) + sorted(sorted.length / 2)) / 2.0
  }

  private def percentile(sorted: Array[Long], fraction: Double): Double =
    if (sorted.isEmpty) 0.0
    else {
      val index = math.min(sorted.length - 1, math.max(0, (sorted.length * fraction).toInt))
      sorted(index).toDouble / 1000.0
    }
}

/** One fork's result for one pool: a rate, or the raw per-op timings. */
private[bench] final case class Sample(opsPerSecond: Double, nanos: List[Long])
