package zoi.pool

import zio.test.TestAspect.{ifEnvSet, sequential}

object MySqlConnectionPoolSpec extends ConnectionPoolContractSpec {
  val backend: JdbcBackend = MySqlBackend
  override def spec        = super.spec @@ ifEnvSet("USE_CONTAINERS") @@ sequential
}
