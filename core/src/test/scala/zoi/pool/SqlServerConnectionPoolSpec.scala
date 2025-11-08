package zoi.pool

import zio.test.TestAspect.{ifEnvSet, sequential}

object SqlServerConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = SqlServerBackend
  override def spec        = super.spec @@ ifEnvSet("USE_CONTAINERS") @@ sequential
}
