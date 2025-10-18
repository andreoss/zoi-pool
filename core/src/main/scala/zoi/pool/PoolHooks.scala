package zoi.pool

/**
 * Behaviour the pool needs but configuration cannot carry: a config stays pure
 * data, so functions and instances are passed here instead.
 */
final case class PoolHooks(
  classify: Throwable => Option[SqlExceptionClassification] = _ => None,
  metrics: PoolMetrics = PoolMetrics.none,
) {

  /** The caller's reading of a failure, falling back to the default one. */
  private[pool] def classification(failure: Throwable): SqlExceptionClassification =
    classify(failure).getOrElse(SqlExceptionClassification.default(failure))
}

object PoolHooks {

  /** The pool's own reading of every failure. */
  val default: PoolHooks = PoolHooks()
}
