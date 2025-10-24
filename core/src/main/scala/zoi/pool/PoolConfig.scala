package zoi.pool

import zio.{Chunk, Duration, durationInt}

/**
 * Pure configuration data for a pool: no functions, no driver instances.
 *
 * Behavioral extensions (exception classification, metrics sinks, a
 * caller-supplied connection source) are wiring, and are passed where the pool
 * is built rather than carried here, so a config stays comparable, renderable
 * and loadable from a file.
 *
 * Construction validates: no invalid `PoolConfig` value can exist, including
 * one produced by `copy`.
 */
final case class PoolConfig(
  url: String,
  username: Option[String] = None,
  password: Option[String] = None,
  driverClassName: Option[String] = None,
  connectionProperties: Map[String, String] = Map.empty,
  poolName: String = PoolConfig.DefaultPoolName,
  maximumPoolSize: Int = 10,
  minimumIdle: Option[Int] = None,
  initialSize: Int = 0,
  connectionTimeout: Duration = 30.seconds,
  validationTimeout: Duration = 5.seconds,
  idleTimeout: Duration = 10.minutes,
  maxLifetime: Duration = 30.minutes,
  keepaliveTime: Duration = Duration.Zero,
  leakDetectionThreshold: Duration = Duration.Zero,
  aliveBypassWindow: Duration = 500.millis,
  shutdownTimeout: Duration = 30.seconds,
  maintenanceInterval: Option[Duration] = None,
  autoCommit: Boolean = true,
  transactionIsolation: Option[TransactionIsolation] = None,
  readOnly: Boolean = false,
  catalog: Option[String] = None,
  schema: Option[String] = None,
  connectionInitSql: Option[String] = None,
  connectionTestQuery: Option[String] = None,
  statementCacheSize: Int = 0,
  failureTracking: Boolean = true,
  jmxEnabled: Boolean = false,
  jmxDomain: String = PoolConfig.DefaultJmxDomain,
) {

  PoolConfig.errorsOf(this) match {
    case errors if errors.nonEmpty => throw new PoolConfigException(errors)
    case _                        => ()
  }

  /** How many idle connections the pool keeps: `maximumPoolSize` by default. */
  def effectiveMinimumIdle: Int = minimumIdle.getOrElse(maximumPoolSize)

  def keepaliveEnabled: Boolean      = keepaliveTime.toNanos > 0L
  def leakDetectionEnabled: Boolean  = leakDetectionThreshold.toNanos > 0L
  def statementCacheEnabled: Boolean = statementCacheSize > 0
  def maxLifetimeEnabled: Boolean    = maxLifetime.toNanos > 0L
  def idleTimeoutEnabled: Boolean    = idleTimeout.toNanos > 0L

  /** How often the housekeeper runs: often enough for the shortest policy. */
  def effectiveMaintenanceInterval: Duration =
    maintenanceInterval.getOrElse {
      val policies = List(idleTimeout, maxLifetime, keepaliveTime, leakDetectionThreshold)
        .map(_.toNanos)
        .filter(_ > 0L)
      val derived  = if (policies.isEmpty) PoolConfig.MaxMaintenanceNanos else policies.min / 4L
      Duration.fromNanos(
        math.min(PoolConfig.MaxMaintenanceNanos, math.max(PoolConfig.MinMaintenanceNanos, derived)),
      )
    }

  /** Renders without url, credentials or driver properties. */
  override def toString: String =
    s"PoolConfig(poolName=$poolName, maximumPoolSize=$maximumPoolSize, " +
      s"minimumIdle=$effectiveMinimumIdle, initialSize=$initialSize, " +
      s"connectionTimeout=$connectionTimeout, idleTimeout=$idleTimeout, " +
      s"maxLifetime=$maxLifetime, keepaliveTime=$keepaliveTime, " +
      s"leakDetectionThreshold=$leakDetectionThreshold, " +
      s"statementCacheSize=$statementCacheSize, failureTracking=$failureTracking, " +
      s"jmxEnabled=$jmxEnabled)"
}

object PoolConfig {

  val DefaultPoolName: String  = "zoi-pool"
  val DefaultJmxDomain: String = "dev.zoi.pool"

  private[pool] val MinMaintenanceNanos: Long = 100L * 1000000L
  private[pool] val MaxMaintenanceNanos: Long = 30L * 1000000000L

  /** Reads a pool configuration from any ZIO config source. */
  val config: zio.Config[PoolConfig] = PoolConfigDescriptor.config

  /** The same descriptor under a dotted path, nested one segment at a time. */
  def configAt(path: String): zio.Config[PoolConfig] =
    path.split('.').iterator.filter(_.nonEmpty).foldRight(config)((segment, nested) => nested.nested(segment))

  /** Builds a config, collecting every violation instead of throwing. */
  def validated(config: => PoolConfig): Either[Chunk[PoolConfigError], PoolConfig] =
    try Right(config)
    catch { case e: PoolConfigException => Left(e.errors) }

  private[pool] def errorsOf(c: PoolConfig): Chunk[PoolConfigError] = {
    val b = Chunk.newBuilder[PoolConfigError]

    def reject(field: String, message: String): Unit = {
      b += PoolConfigError(field, message)
      ()
    }

    def requireText(field: String, value: String): Unit =
      if (value == null || value.trim.isEmpty) reject(field, "must not be blank")

    def requireNonNegative(field: String, value: Duration): Unit =
      if (value.toNanos < 0L) reject(field, "must not be negative")

    def requirePositive(field: String, value: Duration): Unit =
      if (value.toNanos <= 0L) reject(field, "must be positive")

    requireText("url", c.url)
    requireText("poolName", c.poolName)

    if (c.maximumPoolSize <= 0) reject("maximumPoolSize", "must be greater than 0")

    c.minimumIdle.foreach { idle =>
      if (idle < 0) reject("minimumIdle", "must not be negative")
      else if (idle > c.maximumPoolSize) reject("minimumIdle", "must not exceed maximumPoolSize")
    }

    if (c.initialSize < 0) reject("initialSize", "must not be negative")
    else if (c.initialSize > c.maximumPoolSize)
      reject("initialSize", "must not exceed maximumPoolSize")

    requirePositive("connectionTimeout", c.connectionTimeout)
    requirePositive("validationTimeout", c.validationTimeout)
    requireNonNegative("idleTimeout", c.idleTimeout)
    requireNonNegative("maxLifetime", c.maxLifetime)
    requireNonNegative("keepaliveTime", c.keepaliveTime)
    requireNonNegative("leakDetectionThreshold", c.leakDetectionThreshold)
    requireNonNegative("aliveBypassWindow", c.aliveBypassWindow)
    requireNonNegative("shutdownTimeout", c.shutdownTimeout)
    c.maintenanceInterval.foreach(interval => requirePositive("maintenanceInterval", interval))

    if (c.statementCacheSize < 0) reject("statementCacheSize", "must not be negative")

    if (c.maxLifetimeEnabled) {
      if (c.keepaliveEnabled && c.keepaliveTime.toNanos >= c.maxLifetime.toNanos)
        reject("keepaliveTime", "must be shorter than maxLifetime")
      if (c.idleTimeoutEnabled && c.idleTimeout.toNanos >= c.maxLifetime.toNanos)
        reject("idleTimeout", "must be shorter than maxLifetime")
    }

    b.result()
  }
}
