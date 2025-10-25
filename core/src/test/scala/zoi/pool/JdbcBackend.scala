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

/** Derby speaks a different dialect, which is the point of keeping it. */
object DerbyBackend extends JdbcBackend {
  val name = "Derby"

  def freshUrl: UIO[String] =
    uniqueSuffix.map(id => s"jdbc:derby:memory:zoi$id;create=true")

  override def selectOne: String = "SELECT 1 FROM SYSIBM.SYSDUMMY1"

  override def createTableSql(table: String): String =
    s"CREATE TABLE $table (id INT PRIMARY KEY, name VARCHAR(64))"
}

/** SQLite is the awkward one: one writer, and state it refuses to change. */
object SQLiteBackend extends JdbcBackend {
  val name = "SQLite"

  def freshUrl: UIO[String] =
    uniqueSuffix.map(id => s"jdbc:sqlite:file:zoi$id?mode=memory&cache=shared")

  override def supportsReadOnly: Boolean = false

  override def supportsCatalog: Boolean = false

  override def supportsSchema: Boolean = false
}
