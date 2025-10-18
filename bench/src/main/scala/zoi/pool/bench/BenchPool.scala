package zoi.pool.bench

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import zio.{Scope, Task, ZIO}
import zoi.pool.{ConnectionPool, PoolConfig}

/** One pool under measurement, driven through its own idiomatic API. */
trait BenchPool {
  def name: String
  def open(settings: BenchSettings): ZIO[Scope, Throwable, BenchPool.Borrow]
}

object BenchPool {

  /** Borrow, use, release — whatever that means for the pool being measured. */
  trait Borrow {
    def apply[A](use: Connection => A): Task[A]
  }

  def byName(name: String): Option[BenchPool] = name match {
    case "zoi"    => Some(Zoi)
    case "hikari" => Some(Hikari)
    case "none"   => Some(NoPool)
    case _        => None
  }

  /** The library under test, used the way a ZIO caller uses it. */
  object Zoi extends BenchPool {
    val name = "zoi"

    def open(settings: BenchSettings): ZIO[Scope, Throwable, Borrow] =
      ConnectionPool
        .scoped(
          PoolConfig(
            url = settings.url,
            username = settings.username,
            password = settings.password,
            maximumPoolSize = settings.maximumPoolSize,
            minimumIdle = Some(settings.minimumIdle),
            initialSize = settings.minimumIdle,
            connectionTimeout = settings.connectionTimeout,
            poolName = "bench",
          ),
        )
        .map { pool =>
          new Borrow {
            def apply[A](use: Connection => A): Task[A] =
              ZIO.scoped(pool.connection.flatMap(c => ZIO.attemptBlocking(use(c))))
          }
        }
  }

  /** The incumbent, used the way a Java caller uses it. */
  object Hikari extends BenchPool {
    val name = "hikari"

    def open(settings: BenchSettings): ZIO[Scope, Throwable, Borrow] =
      ZIO
        .acquireRelease(ZIO.attemptBlocking(dataSource(settings)))(ds => ZIO.succeed(ds.close()))
        .map { source =>
          new Borrow {
            def apply[A](use: Connection => A): Task[A] =
              ZIO.attemptBlocking {
                val connection = source.getConnection()
                try use(connection)
                finally connection.close()
              }
          }
        }

    private def dataSource(settings: BenchSettings): HikariDataSource = {
      val config = new HikariConfig()
      config.setJdbcUrl(settings.url)
      settings.username.foreach(config.setUsername)
      settings.password.foreach(config.setPassword)
      config.setMaximumPoolSize(settings.maximumPoolSize)
      config.setMinimumIdle(settings.minimumIdle)
      config.setConnectionTimeout(settings.connectionTimeout.toMillis)
      config.setPoolName("bench-hikari")
      new HikariDataSource(config)
    }
  }

  /** No pooling at all: the floor every pool has to beat. */
  object NoPool extends BenchPool {
    val name = "none"

    def open(settings: BenchSettings): ZIO[Scope, Throwable, Borrow] =
      ZIO.succeed {
        val properties = new Properties()
        settings.username.foreach(properties.setProperty("user", _))
        settings.password.foreach(properties.setProperty("password", _))

        new Borrow {
          def apply[A](use: Connection => A): Task[A] =
            ZIO.attemptBlocking {
              val connection = DriverManager.getConnection(settings.url, properties)
              try use(connection)
              finally connection.close()
            }
        }
      }
  }
}
