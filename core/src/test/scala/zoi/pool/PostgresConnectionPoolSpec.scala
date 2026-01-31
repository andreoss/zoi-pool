package zoi.pool

object PostgresConnectionPoolSpec extends ContainerContractSpec {
  val backend: JdbcBackend = PostgresBackend
}
