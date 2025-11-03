package zoi.pool

import java.io.PrintWriter
import java.sql.{Connection, SQLFeatureNotSupportedException}
import java.util.logging.Logger
import javax.sql.DataSource

/**
 * The pool seen as a plain `javax.sql.DataSource`, so a consumer that knows
 * nothing about ZIO can use it: `getConnection` borrows and `close` returns.
 */
final private[pool] class PoolDataSource(pool: ConnectionPoolLive, config: PoolConfig)
    extends DataSource {

  @volatile private var writer: PrintWriter = null
  @volatile private var loginTimeoutSeconds = 0

  override def getConnection: Connection = pool.borrowUnsafe()

  override def getConnection(username: String, password: String): Connection =
    throw new SQLFeatureNotSupportedException(
      s"${config.poolName} - credentials are fixed by the pool configuration",
    )

  override def getLogWriter: PrintWriter = writer

  override def setLogWriter(out: PrintWriter): Unit = writer = out

  override def setLoginTimeout(seconds: Int): Unit = loginTimeoutSeconds = seconds

  override def getLoginTimeout: Int = loginTimeoutSeconds

  override def getParentLogger: Logger =
    throw new SQLFeatureNotSupportedException(s"${config.poolName} - no parent logger")

  override def unwrap[T](iface: Class[T]): T =
    if (iface.isInstance(this)) iface.cast(this)
    else throw new SQLFeatureNotSupportedException(s"${config.poolName} - not a wrapper for $iface")

  override def isWrapperFor(iface: Class[_]): Boolean = iface.isInstance(this)
}
