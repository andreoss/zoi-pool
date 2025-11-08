package zoi.pool

import zio.test.TestAspect.{ifEnvSet, sequential}

object OracleConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = OracleBackend
  override def spec        = super.spec @@ ifEnvSet("USE_CONTAINERS") @@ sequential
}
