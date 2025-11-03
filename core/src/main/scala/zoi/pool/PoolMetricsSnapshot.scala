package zoi.pool

/** Everything the pool has counted, plus what it holds right now. */
final case class PoolMetricsSnapshot(
    connectionsCreated: Long = 0L,
    connectionsClosed: Long = 0L,
    connectionsRetired: Long = 0L,
    acquires: Long = 0L,
    acquireNanos: Long = 0L,
    parkedAcquires: Long = 0L,
    parkedNanos: Long = 0L,
    acquireTimeouts: Long = 0L,
    leaksSuspected: Long = 0L,
    active: Int = 0,
    idle: Int = 0,
    total: Int = 0,
    waiting: Int = 0,
) {

  def averageAcquireNanos: Double =
    if (acquires > 0L) acquireNanos.toDouble / acquires.toDouble else 0.0

  def averageParkedNanos: Double =
    if (parkedAcquires > 0L) parkedNanos.toDouble / parkedAcquires.toDouble else 0.0

  private[pool] def withState(state: PoolState): PoolMetricsSnapshot =
    copy(active = state.active, idle = state.idle, total = state.total, waiting = state.waiting)
}
