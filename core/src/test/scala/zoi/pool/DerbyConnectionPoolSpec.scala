package zoi.pool

object DerbyConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = DerbyBackend
}
