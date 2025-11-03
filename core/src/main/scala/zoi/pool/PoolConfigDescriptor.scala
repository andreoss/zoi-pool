package zoi.pool

import zio.{Config, Duration, durationInt}

/**
 * Reads a pool configuration from any ZIO config source: HOCON, environment,
 * properties, whatever the application already uses.
 *
 * It is built from ZIO's own `Config`, so the core still depends on nothing but
 * ZIO and the JDK. The fields are grouped as they are named, which also keeps
 * each group inside what the older Scala's tuples can hold.
 */
private[pool] object PoolConfigDescriptor {

  final private case class Connection(
      url: String,
      username: Option[String],
      password: Option[String],
      driverClassName: Option[String],
      properties: Map[String, String],
      poolName: String,
  )

  final private case class Sizing(maximumPoolSize: Int, minimumIdle: Option[Int], initialSize: Int)

  final private case class Timeouts(
      connectionTimeout: Duration,
      validationTimeout: Duration,
      idleTimeout: Duration,
      maxLifetime: Duration,
      keepaliveTime: Duration,
      leakDetectionThreshold: Duration,
      aliveBypassWindow: Duration,
      shutdownTimeout: Duration,
      maintenanceInterval: Option[Duration],
  )

  final private case class State(
      autoCommit: Boolean,
      transactionIsolation: Option[TransactionIsolation],
      readOnly: Boolean,
      catalog: Option[String],
      schema: Option[String],
      connectionInitSql: Option[String],
      connectionTestQuery: Option[String],
  )

  final private case class Features(
      statementCacheSize: Int,
      failureTracking: Boolean,
      jmxEnabled: Boolean,
      jmxDomain: String,
  )

  private val connection: Config[Connection] =
    (Config.string("url") ++
      Config.string("username").optional ++
      Config.string("password").optional ++
      Config.string("driverClassName").optional ++
      Config.table("connectionProperties", Config.string).withDefault(Map.empty[String, String]) ++
      Config.string("poolName").withDefault(PoolConfig.DefaultPoolName))
      .map((Connection.apply _).tupled)

  private val sizing: Config[Sizing] =
    (Config.int("maximumPoolSize").withDefault(10) ++
      Config.int("minimumIdle").optional ++
      Config.int("initialSize").withDefault(0))
      .map((Sizing.apply _).tupled)

  private val timeouts: Config[Timeouts] =
    (Config.duration("connectionTimeout").withDefault(30.seconds) ++
      Config.duration("validationTimeout").withDefault(5.seconds) ++
      Config.duration("idleTimeout").withDefault(10.minutes) ++
      Config.duration("maxLifetime").withDefault(30.minutes) ++
      Config.duration("keepaliveTime").withDefault(Duration.Zero) ++
      Config.duration("leakDetectionThreshold").withDefault(Duration.Zero) ++
      Config.duration("aliveBypassWindow").withDefault(500.millis) ++
      Config.duration("shutdownTimeout").withDefault(30.seconds) ++
      Config.duration("maintenanceInterval").optional)
      .map((Timeouts.apply _).tupled)

  private val state: Config[State] =
    (Config.boolean("autoCommit").withDefault(true) ++
      isolation ++
      Config.boolean("readOnly").withDefault(false) ++
      Config.string("catalog").optional ++
      Config.string("schema").optional ++
      Config.string("connectionInitSql").optional ++
      Config.string("connectionTestQuery").optional)
      .map((State.apply _).tupled)

  private val features: Config[Features] =
    (Config.int("statementCacheSize").withDefault(0) ++
      Config.boolean("failureTracking").withDefault(true) ++
      Config.boolean("jmxEnabled").withDefault(false) ++
      Config.string("jmxDomain").withDefault(PoolConfig.DefaultJmxDomain))
      .map((Features.apply _).tupled)

  private def isolation: Config[Option[TransactionIsolation]] =
    Config
      .string("transactionIsolation")
      .optional
      .mapOrFail {
        case None       => Right(None)
        case Some(name) =>
          TransactionIsolation
            .fromName(name)
            .map(level => Right(Some(level)))
            .getOrElse(
              Left(Config.Error.InvalidData(message = s"not a transaction isolation level: $name")),
            )
      }

  val config: Config[PoolConfig] =
    (connection ++ sizing ++ timeouts ++ state ++ features).mapOrFail {
      case (connection, sizing, timeouts, state, features) =>
        assemble(connection, sizing, timeouts, state, features)
    }

  private def assemble(
      connection: Connection,
      sizing: Sizing,
      timeouts: Timeouts,
      state: State,
      features: Features,
  ): Either[Config.Error, PoolConfig] =
    PoolConfig
      .validated(
        PoolConfig(
          url = connection.url,
          username = connection.username,
          password = connection.password,
          driverClassName = connection.driverClassName,
          connectionProperties = connection.properties,
          poolName = connection.poolName,
          maximumPoolSize = sizing.maximumPoolSize,
          minimumIdle = sizing.minimumIdle,
          initialSize = sizing.initialSize,
          connectionTimeout = timeouts.connectionTimeout,
          validationTimeout = timeouts.validationTimeout,
          idleTimeout = timeouts.idleTimeout,
          maxLifetime = timeouts.maxLifetime,
          keepaliveTime = timeouts.keepaliveTime,
          leakDetectionThreshold = timeouts.leakDetectionThreshold,
          aliveBypassWindow = timeouts.aliveBypassWindow,
          shutdownTimeout = timeouts.shutdownTimeout,
          maintenanceInterval = timeouts.maintenanceInterval,
          autoCommit = state.autoCommit,
          transactionIsolation = state.transactionIsolation,
          readOnly = state.readOnly,
          catalog = state.catalog,
          schema = state.schema,
          connectionInitSql = state.connectionInitSql,
          connectionTestQuery = state.connectionTestQuery,
          statementCacheSize = features.statementCacheSize,
          failureTracking = features.failureTracking,
          jmxEnabled = features.jmxEnabled,
          jmxDomain = features.jmxDomain,
        ),
      )
      .left
      .map(errors => Config.Error.InvalidData(message = errors.mkString("; ")))
}
