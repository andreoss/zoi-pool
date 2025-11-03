package zoi.pool

/** How many connections the pool holds right now, and in which state. */
final case class PoolState(
    active: Int,
    idle: Int,
    total: Int,
    waiting: Int,
    suspended: Boolean,
    shutdown: Boolean,
)

object PoolState {
  val empty: PoolState = PoolState(0, 0, 0, 0, suspended = false, shutdown = false)
}
