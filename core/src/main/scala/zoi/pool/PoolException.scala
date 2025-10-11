package zoi.pool

import java.sql.{SQLNonTransientConnectionException, SQLTransientConnectionException}

import zio.Duration

/** No connection became available before `connectionTimeout` elapsed. */
final class PoolTimeoutException(poolName: String, timeout: Duration)
    extends SQLTransientConnectionException(
      s"$poolName - connection is not available, request timed out after ${timeout.toMillis}ms",
      PoolException.ConnectionFailure,
    )

/** The pool was shut down, so it can no longer hand out connections. */
final class PoolShutdownException(poolName: String)
    extends SQLNonTransientConnectionException(
      s"$poolName - pool has been shut down",
      PoolException.ConnectionDoesNotExist,
    )

/** A physical connection could not be opened. */
final class ConnectionCreationException(poolName: String, cause: Throwable)
    extends SQLTransientConnectionException(
      s"$poolName - failed to open a connection",
      PoolException.UnableToConnect,
      cause,
    )

private[pool] object PoolException {
  val ConnectionFailure: String      = "08006"
  val ConnectionDoesNotExist: String = "08003"
  val UnableToConnect: String        = "08001"
}
