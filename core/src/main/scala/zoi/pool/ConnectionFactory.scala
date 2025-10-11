package zoi.pool

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import zio.{IO, UIO, ZIO}

/** Opens and closes physical connections for one pool. */
private[pool] final class ConnectionFactory(config: PoolConfig) {

  private val properties: Properties = {
    val props = new Properties()
    config.connectionProperties.foreach { case (key, value) => props.setProperty(key, value) }
    config.username.foreach(props.setProperty("user", _))
    config.password.foreach(props.setProperty("password", _))
    props
  }

  def open: IO[SQLException, Connection] =
    ZIO
      .attemptBlocking(DriverManager.getConnection(config.url, properties))
      .mapError(failure => new ConnectionCreationException(config.poolName, failure))

  def close(connection: Connection): UIO[Unit] =
    ZIO.attemptBlocking(connection.close()).ignore
}

private[pool] object ConnectionFactory {

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
