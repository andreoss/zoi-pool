package zoi.pool

object H2ConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = H2Backend
}
