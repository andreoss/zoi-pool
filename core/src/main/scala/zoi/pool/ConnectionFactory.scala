package zoi.pool

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import zio.{Duration, IO, UIO, ZIO}

/** Opens, configures, validates and closes physical connections for one pool. */
final private[pool] class ConnectionFactory(config: PoolConfig) {

  private val properties: Properties = {
    val props = new Properties()
    config.connectionProperties.foreach { case (key, value) => props.setProperty(key, value) }
    config.username.foreach(props.setProperty("user", _))
    config.password.foreach(props.setProperty("password", _))
    props
  }

  private val validationTimeoutSeconds =
    math.max(1, config.validationTimeout.toMillis / 1000L).toInt

  def open: IO[SQLException, Connection] =
    ZIO
      .attemptBlocking(DriverManager.getConnection(config.url, properties))
      .mapError(failure => new ConnectionCreationException(config.poolName, failure))

  /** Applies the configured connection state to a freshly opened connection. */
  def configure(connection: Connection): IO[SQLException, Unit] =
    ZIO
      .attemptBlocking {
        if (connection.getAutoCommit != config.autoCommit)
          connection.setAutoCommit(config.autoCommit)
        config.transactionIsolation
          .foreach(level => connection.setTransactionIsolation(level.jdbcLevel))
        if (config.readOnly) connection.setReadOnly(true)
        config.catalog.foreach(connection.setCatalog)
        config.schema.foreach(connection.setSchema)
        config.connectionInitSql.foreach { sql =>
          val statement = connection.createStatement()
          try statement.execute(sql)
          finally statement.close()
        }
      }
      .mapError(failure => new ConnectionCreationException(config.poolName, failure))

  /**
   * Puts a returned connection back into its configured state. A borrower that
   * changed nothing costs no round trips and no executor hop: only the local
   * warning clear runs, inline.
   */
  def reset(connection: Connection, dirty: Boolean): UIO[Boolean] =
    if (!dirty) ZIO.succeed(clearWarnings(connection))
    else
      ZIO
        .attemptBlocking {
          if (!connection.getAutoCommit) {
            connection.rollback()
            connection.setAutoCommit(config.autoCommit)
          }
          config.transactionIsolation.foreach { level =>
            if (connection.getTransactionIsolation != level.jdbcLevel)
              connection.setTransactionIsolation(level.jdbcLevel)
          }
          if (connection.isReadOnly != config.readOnly) connection.setReadOnly(config.readOnly)
          config.catalog.foreach(catalog =>
            if (connection.getCatalog != catalog) connection.setCatalog(catalog),
          )
          config.schema
            .foreach(schema => if (connection.getSchema != schema) connection.setSchema(schema))
          connection.clearWarnings()
          true
        }
        .catchAll(_ => ZIO.succeed(false))

  private def clearWarnings(connection: Connection): Boolean =
    try {
      connection.clearWarnings()
      true
    } catch { case _: SQLException => false }

  /** Checks a connection is still usable, by test query or by `isValid`. */
  def validate(connection: Connection): UIO[Boolean] =
    ZIO
      .attemptBlocking {
        config.connectionTestQuery match {
          case Some(query) =>
            val statement = connection.createStatement()
            try {
              statement.setQueryTimeout(validationTimeoutSeconds)
              statement.execute(query)
              true
            } finally statement.close()
          case None        => connection.isValid(validationTimeoutSeconds)
        }
      }
      .catchAll(_ => ZIO.succeed(false))
      .timeoutTo(false)(identity)(config.validationTimeout)

  def close(connection: Connection): UIO[Unit] =
    ZIO.attemptBlocking(connection.close()).ignore

  /** Interrupts a connection a borrower is still holding, for shutdown. */
  def abort(connection: Connection): UIO[Unit] =
    ZIO
      .attemptBlocking(connection.abort(ConnectionFactory.CallerRuns))
      .catchAll(_ => close(connection))
      .unit
}

private[pool] object ConnectionFactory {

  private val CallerRuns: java.util.concurrent.Executor =
    (command: Runnable) => command.run()

  def make(config: PoolConfig): IO[SQLException, ConnectionFactory] =
    loadDriver(config).as(new ConnectionFactory(config))

  private def loadDriver(config: PoolConfig): IO[SQLException, Unit] =
    ZIO
      .foreachDiscard(config.driverClassName) { name =>
        ZIO.attemptBlocking(Class.forName(name, true, classLoader)).unit
      }
      .mapError(failure => new ConnectionCreationException(config.poolName, failure))

  private def classLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(getClass.getClassLoader)
}
