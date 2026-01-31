package zoi.pool

object MariaDbConnectionPoolSpec extends ContainerContractSpec {
  val backend: JdbcBackend = MariaDbBackend
}
