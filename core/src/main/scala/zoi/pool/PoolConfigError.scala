package zoi.pool

import zio.Chunk

/** One rejected configuration field and why it was rejected. */
final case class PoolConfigError(field: String, message: String) {
  override def toString: String = s"$field: $message"
}

/** Raised when a [[PoolConfig]] is built from values that cannot hold. */
final class PoolConfigException(val errors: Chunk[PoolConfigError])
    extends IllegalArgumentException(PoolConfigException.render(errors))

object PoolConfigException {
  private def render(errors: Chunk[PoolConfigError]): String =
    errors.mkString("invalid pool configuration: ", "; ", "")
}
