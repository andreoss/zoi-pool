package zoi.pool

import zio.{Random, UIO, ZIO}

/** One database the contract suite can be run against. */
trait JdbcBackend {

  def name: String

  /** A URL nothing else in the run is using. */
  def freshUrl: UIO[String]

  def config(url: String): PoolConfig = PoolConfig(url)

  def selectOne: String = "SELECT 1"

  def createTableSql(table: String): String =
    s"CREATE TABLE $table (id INT PRIMARY KEY, name VARCHAR(64))"

  def dropTableSql(table: String): String = s"DROP TABLE $table"

  def supportsReadOnly: Boolean = true

  def supportsCatalog: Boolean = true

  def supportsSchema: Boolean = true

  protected def uniqueSuffix: UIO[String] =
    Random.nextUUID.map(_.toString.replace("-", ""))
}

object H2Backend extends JdbcBackend {
  val name = "H2"

  def freshUrl: UIO[String] =
    uniqueSuffix.map(id => s"jdbc:h2:mem:zoi$id;DB_CLOSE_DELAY=-1")
}
