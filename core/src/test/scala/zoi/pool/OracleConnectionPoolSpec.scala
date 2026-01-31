package zoi.pool

object OracleConnectionPoolSpec extends ContainerContractSpec {
  val backend: JdbcBackend = OracleBackend
}
