# zoi-pool

A JDBC connection pool for [ZIO 2](https://zio.dev) that drops into an existing
ZIO project without changing anything above the layer that builds the
`DataSource`.

- **Two surfaces, one pool.** `ConnectionPool` for ZIO callers, a plain
  `javax.sql.DataSource` for everything else. Both are layers, and closing the
  layer's scope shuts the pool down.
- **Built on ZIO primitives.** `Scope` owns the lifecycle, fibres run the
  maintenance, and blocking JDBC calls stay on the blocking executor.
- **Works with the usual stack.** zio-quill, doobie, Slick, Flyway and plain
  JDBC take the `DataSource`; zio-jdbc gets an adapter.
- **Cross-built** for Scala 2.13 and 3, compiled with `-Werror`.

```scala
libraryDependencies += "dev.zoi" %% "zoi-pool" % "0.1.0"
```

## Replacing another pool

Anything that takes a `DataSource` keeps taking one. Only the layer changes:

```scala
// before
val dataSource = ZLayer.scoped(ZIO.fromAutoCloseable(ZIO.attempt(new JdbcDataSource(config))))

// after
val dataSource = ConnectionPool.dataSourceLayer(PoolConfig(url, maximumPoolSize = 20))
```

With zio-quill that is the whole migration:

```scala
import io.getquill._
import io.getquill.jdbczio.Quill

val quill = Quill.Postgres.fromNamingStrategy(SnakeCase)

myApp.provide(ConnectionPool.dataSourceLayer(config), quill)
```

## Configuration from a file

`PoolConfig` reads from any ZIO config source, so HOCON, environment variables
and system properties all work without another dependency:

```scala
ConnectionPool.layerFromConfig("zoi.pool")
```

```hocon
zoi.pool {
  url = "jdbc:postgresql://localhost:5432/orders"
  maximumPoolSize = 20
  minimumIdle = 5
  connectionTimeout = 10s
  leakDetectionThreshold = 30s
}
```

## Observability

```scala
val hooks = PoolHooks(metrics = PoolMetrics.zio("orders"))

ConnectionPool.layer(config.copy(jmxEnabled = true), hooks)
```

`PoolMetrics.zio` publishes into ZIO's metric registry; `pool.metrics` returns a
snapshot directly; `jmxEnabled` registers a management bean reporting the same
numbers, with suspend and resume. Nothing reported carries a URL, a credential
or statement text.

## Running the tests

```bash
sbt test                      # unit and embedded databases: H2, Derby, SQLite
USE_CONTAINERS=1 sbt test     # adds PostgreSQL, MySQL and MariaDB in containers
sbt "bench/run"               # zoi-pool against a baseline pool and no pool at all
```

## License

LGPL-3.0
