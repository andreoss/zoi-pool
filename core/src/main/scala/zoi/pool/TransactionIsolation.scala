package zoi.pool

import java.sql.Connection
import java.util.Locale

import zio.Chunk

/** A JDBC transaction isolation level, named as the driver names it. */
sealed abstract class TransactionIsolation(val jdbcLevel: Int, val name: String) {
  override def toString: String = name
}

object TransactionIsolation {

  case object NoTransactions
      extends TransactionIsolation(Connection.TRANSACTION_NONE, "TRANSACTION_NONE")

  case object ReadUncommitted
      extends TransactionIsolation(
        Connection.TRANSACTION_READ_UNCOMMITTED,
        "TRANSACTION_READ_UNCOMMITTED",
      )

  case object ReadCommitted
      extends TransactionIsolation(
        Connection.TRANSACTION_READ_COMMITTED,
        "TRANSACTION_READ_COMMITTED",
      )

  case object RepeatableRead
      extends TransactionIsolation(
        Connection.TRANSACTION_REPEATABLE_READ,
        "TRANSACTION_REPEATABLE_READ",
      )

  case object Serializable
      extends TransactionIsolation(Connection.TRANSACTION_SERIALIZABLE, "TRANSACTION_SERIALIZABLE")

  val all: Chunk[TransactionIsolation] =
    Chunk(NoTransactions, ReadUncommitted, ReadCommitted, RepeatableRead, Serializable)

  /** Parses a level from the JDBC name, the bare name, or either unpunctuated. */
  def fromName(name: String): Option[TransactionIsolation] = {
    val wanted = normalize(name)
    all.find(level => normalize(level.name) == wanted)
  }

  /** Parses a level from the numeric constant a driver reports. */
  def fromJdbcLevel(level: Int): Option[TransactionIsolation] =
    all.find(_.jdbcLevel == level)

  private def normalize(raw: String): String =
    raw.trim.toUpperCase(Locale.ROOT).replace("_", "").stripPrefix("TRANSACTION")
}
