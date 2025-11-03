package zoi.pool

import java.sql.{Connection, PreparedStatement}

/**
 * A physical connection plus the bookkeeping the pool keeps about it.
 *
 * The fields are plain volatiles written by whoever holds the connection at the
 * time; the pool's invariants live in the hand-off core, not here.
 */
final private[pool] class PooledConnection(
    val raw: Connection,
    val createdAtNanos: Long,
    val maxLifetimeNanos: Long,
    val statementCache: Option[StatementCache],
) {
  @volatile var lastReturnedNanos: Long  = createdAtNanos
  @volatile var lastValidatedNanos: Long = createdAtNanos
  @volatile var borrowedAtNanos: Long    = 0L
  @volatile var borrowed: Boolean        = false
  @volatile var broken: Boolean          = false
  @volatile var stateDirty: Boolean      = false
  @volatile var leakReported: Boolean    = false

  def prepare(sql: String): PreparedStatement =
    statementCache match {
      case Some(cache) => cache.prepare(sql)
      case None        => raw.prepareStatement(sql)
    }

  def expiredAt(nowNanos: Long): Boolean =
    maxLifetimeNanos > 0L && nowNanos - createdAtNanos >= maxLifetimeNanos

  def idleSince(nowNanos: Long): Long = nowNanos - lastReturnedNanos

  def close(): Unit = {
    try statementCache.foreach(_.close())
    catch { case _: Throwable => () }
    raw.close()
  }
}
