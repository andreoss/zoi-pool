package zoi.pool

import zio.test.TestAspect.{ifEnvSet, sequential}

object MariaDbConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = MariaDbBackend
  override def spec        = super.spec @@ ifEnvSet("USE_CONTAINERS") @@ sequential
}
