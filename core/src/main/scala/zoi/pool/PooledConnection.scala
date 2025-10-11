package zoi.pool

import java.sql.{Connection, PreparedStatement}

/** A physical connection plus the bookkeeping the pool keeps about it. */
private[pool] final class PooledConnection(
  val raw: Connection,
  val createdAtNanos: Long,
) {
  @volatile var lastReturnedNanos: Long = createdAtNanos
  @volatile var broken: Boolean         = false
  @volatile var stateDirty: Boolean     = false

  def prepare(sql: String): PreparedStatement = raw.prepareStatement(sql)
}
