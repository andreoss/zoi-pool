package zoi.pool

object MySqlConnectionPoolSpec extends ContainerContractSpec {
  val backend: JdbcBackend = MySqlBackend
}
