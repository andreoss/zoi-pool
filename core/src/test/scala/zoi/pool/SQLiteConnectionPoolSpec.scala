package zoi.pool

object SQLiteConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = SQLiteBackend
}
