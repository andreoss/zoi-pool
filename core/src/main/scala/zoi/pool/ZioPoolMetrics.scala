package zoi.pool

import zio.metrics.{Metric, MetricLabel}
import zio.{Duration, Scope, UIO, ZIO, durationInt}

/**
 * Publishes the pool's signals into ZIO's metric registry, so anything that
 * already scrapes ZIO metrics sees the pool without extra wiring.
 *
 * Counting stays in memory on the acquire path; a fibre installed with the pool
 * copies the numbers into the registry, so the hot path never runs an effect.
 */
final private[pool] class ZioPoolMetrics(poolName: String, interval: Duration) extends PoolMetrics {

  private val backing = PoolMetrics.recording
  private val labels  = Set(MetricLabel("pool", poolName))

  private val active   = gauge("zoi_pool_connections_active")
  private val idle     = gauge("zoi_pool_connections_idle")
  private val total    = gauge("zoi_pool_connections_total")
  private val waiting  = gauge("zoi_pool_borrowers_waiting")
  private val created  = gauge("zoi_pool_connections_created_total")
  private val closed   = gauge("zoi_pool_connections_closed_total")
  private val retired  = gauge("zoi_pool_connections_retired_total")
  private val taken    = gauge("zoi_pool_acquires_total")
  private val timeouts = gauge("zoi_pool_acquire_timeouts_total")
  private val leaks    = gauge("zoi_pool_leaks_suspected_total")
  private val meanWait = gauge("zoi_pool_acquire_seconds_mean")
  private val meanPark = gauge("zoi_pool_parked_seconds_mean")

  def connectionCreated(): Unit                            = backing.connectionCreated()
  def connectionClosed(): Unit                             = backing.connectionClosed()
  def connectionRetired(): Unit                            = backing.connectionRetired()
  def acquireTimedOut(): Unit                              = backing.acquireTimedOut()
  def leakSuspected(): Unit                                = backing.leakSuspected()
  def acquireSucceeded(nanos: Long, parked: Boolean): Unit = backing.acquireSucceeded(nanos, parked)
  def counters: PoolMetricsSnapshot                        = backing.counters

  override def install(snapshot: UIO[PoolMetricsSnapshot]): ZIO[Scope, Nothing, Unit] =
    (snapshot.flatMap(publish) *> ZIO.sleep(interval)).forever.forkScoped.unit

  private def publish(sample: PoolMetricsSnapshot): UIO[Unit] =
    active.update(sample.active.toDouble) *>
      idle.update(sample.idle.toDouble) *>
      total.update(sample.total.toDouble) *>
      waiting.update(sample.waiting.toDouble) *>
      created.update(sample.connectionsCreated.toDouble) *>
      closed.update(sample.connectionsClosed.toDouble) *>
      retired.update(sample.connectionsRetired.toDouble) *>
      taken.update(sample.acquires.toDouble) *>
      timeouts.update(sample.acquireTimeouts.toDouble) *>
      leaks.update(sample.leaksSuspected.toDouble) *>
      meanWait.update(sample.averageAcquireNanos / 1000000000.0) *>
      meanPark.update(sample.averageParkedNanos / 1000000000.0)

  private def gauge(name: String): Metric.Gauge[Double] = Metric.gauge(name).tagged(labels)
}

private[pool] object ZioPoolMetrics {
  val DefaultInterval: Duration = 10.seconds
}
