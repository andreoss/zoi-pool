package zoi.pool

object SqlServerConnectionPoolSpec extends ContainerContractSpec {
  val backend: JdbcBackend = SqlServerBackend
}
