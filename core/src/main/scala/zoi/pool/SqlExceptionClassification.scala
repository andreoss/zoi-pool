package zoi.pool

import java.sql.{
  SQLException,
  SQLNonTransientConnectionException,
  SQLRecoverableException,
  SQLTransientConnectionException,
}

/** Whether a failure killed the connection or only the statement. */
sealed trait SqlExceptionClassification
object SqlExceptionClassification {

  /** The connection is unusable and must not go back into the pool. */
  case object Fatal extends SqlExceptionClassification

  /** The statement failed; the connection is still good. */
  case object Recoverable extends SqlExceptionClassification

  private val FatalStates: Set[String] =
    Set("57P01", "57P02", "57P03", "01002", "JZ0C0", "JZ0C1")

  private val FatalCodes: Set[Int] = Set(500150, 2399)

  /**
   * The default reading of a JDBC failure: SQLState class 08 is a connection
   * exception, and the connection-shaped exception types speak for themselves.
   */
  def default(failure: Throwable): SqlExceptionClassification =
    failure match {
      case _: SQLTransientConnectionException    => Fatal
      case _: SQLNonTransientConnectionException => Fatal
      case _: SQLRecoverableException            => Fatal
      case sql: SQLException                     => fromState(sql)
      case _                                     => Recoverable
    }

  private def fromState(failure: SQLException): SqlExceptionClassification = {
    val state = Option(failure.getSQLState).getOrElse("")
    if (state.startsWith("08") || FatalStates.contains(state)) Fatal
    else if (FatalCodes.contains(failure.getErrorCode)) Fatal
    else Option(failure.getNextException).map(fromState).getOrElse(Recoverable)
  }
}
