package zoi.pool

import zio.test.TestAspect.{ifEnvSet, sequential}

object PostgresConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = PostgresBackend
  override def spec        = super.spec @@ ifEnvSet("USE_CONTAINERS") @@ sequential
}
