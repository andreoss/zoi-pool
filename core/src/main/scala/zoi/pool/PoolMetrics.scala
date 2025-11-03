package zoi.pool

import java.util.concurrent.atomic.LongAdder

import zio.{Duration, Scope, UIO, ZIO}

/**
 * Where the pool reports what it did.
 *
 * The methods are plain and synchronous so the default does nothing at all and
 * costs a single virtual call on the acquire path. Nothing here carries a URL,
 * a credential or a statement.
 */
trait PoolMetrics {
  def connectionCreated(): Unit
  def connectionClosed(): Unit
  def connectionRetired(): Unit
  def acquireSucceeded(nanos: Long, parked: Boolean): Unit
  def acquireTimedOut(): Unit
  def leakSuspected(): Unit

  /** Installs anything the implementation needs for the pool's lifetime. */
  def install(snapshot: UIO[PoolMetricsSnapshot]): ZIO[Scope, Nothing, Unit] =
    ZIO.unit

  /** What has been counted so far; pool gauges are filled in by the pool. */
  def counters: PoolMetricsSnapshot
}

object PoolMetrics {

  /** Counts nothing, the default. */
  val none: PoolMetrics = new PoolMetrics {
    def connectionCreated(): Unit                            = ()
    def connectionClosed(): Unit                             = ()
    def connectionRetired(): Unit                            = ()
    def acquireSucceeded(nanos: Long, parked: Boolean): Unit = ()
    def acquireTimedOut(): Unit                              = ()
    def leakSuspected(): Unit                                = ()
    def counters: PoolMetricsSnapshot                        = PoolMetricsSnapshot()
  }

  /** Counts in memory, for a caller that reads snapshots directly. */
  def recording: PoolMetrics = new Recording

  /** Counts into ZIO's metric registry under the pool's name. */
  def zio(poolName: String, interval: Duration = ZioPoolMetrics.DefaultInterval): PoolMetrics =
    new ZioPoolMetrics(poolName, interval)

  final private class Recording extends PoolMetrics {
    private val created  = new LongAdder
    private val closed   = new LongAdder
    private val retired  = new LongAdder
    private val taken    = new LongAdder
    private val takenNs  = new LongAdder
    private val parked   = new LongAdder
    private val parkedNs = new LongAdder
    private val timeouts = new LongAdder
    private val leaks    = new LongAdder

    def connectionCreated(): Unit = created.increment()
    def connectionClosed(): Unit  = closed.increment()
    def connectionRetired(): Unit = retired.increment()
    def acquireTimedOut(): Unit   = timeouts.increment()
    def leakSuspected(): Unit     = leaks.increment()

    def acquireSucceeded(nanos: Long, wasParked: Boolean): Unit = {
      taken.increment()
      takenNs.add(nanos)
      if (wasParked) {
        parked.increment()
        parkedNs.add(nanos)
      }
    }

    def counters: PoolMetricsSnapshot =
      PoolMetricsSnapshot(
        connectionsCreated = created.sum(),
        connectionsClosed = closed.sum(),
        connectionsRetired = retired.sum(),
        acquires = taken.sum(),
        acquireNanos = takenNs.sum(),
        parkedAcquires = parked.sum(),
        parkedNanos = parkedNs.sum(),
        acquireTimeouts = timeouts.sum(),
        leaksSuspected = leaks.sum(),
      )
  }
}
