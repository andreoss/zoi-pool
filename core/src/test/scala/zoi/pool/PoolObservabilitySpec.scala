package zoi.pool

import java.lang.management.ManagementFactory

import zio.test.TestAspect._
import zio.test._
import zio.{Promise, Scope, ZIO, durationInt}

/** Metrics, the ZIO metric binding and the management bean. */
object PoolObservabilitySpec extends ZIOSpecDefault {

  private val backend = H2Backend

  private def pool(
      config: PoolConfig,
      hooks: PoolHooks,
  ): ZIO[Scope, Throwable, ConnectionPoolLive] = ConnectionPoolLive.scoped(config, hooks)

  private def recorded = PoolHooks(metrics = PoolMetrics.recording)

  def spec = suite("observability")(
    suite("metrics port")(
      test("the default counts nothing and costs nothing") {
        for {
          url  <- backend.freshUrl
          snap <- ZIO.scoped {
            pool(backend.config(url), PoolHooks.default).flatMap { p =>
              ZIO.scoped(p.connection) *> p.metrics
            }
          }
        } yield assertTrue(
          snap.connectionsCreated == 0L,
          snap.acquires == 0L,
          snap.idle == 1,
          snap.total == 1,
        )
      },
      test("a recording port counts creation, acquires and closes") {
        for {
          url  <- backend.freshUrl
          snap <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 1), recorded).flatMap { p =>
              ZIO.scoped(p.connection) *> ZIO.scoped(p.connection) *> p.metrics
            }
          }
        } yield assertTrue(
          snap.connectionsCreated == 1L,
          snap.acquires == 2L,
          snap.connectionsClosed == 0L,
          snap.acquireNanos > 0L,
          snap.averageAcquireNanos > 0.0,
        )
      } @@ withLiveClock,
      test("gauges report what the pool is holding") {
        for {
          url  <- backend.freshUrl
          snap <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 4), recorded).flatMap { p =>
              ZIO.scoped(p.connection *> p.metrics)
            }
          }
        } yield assertTrue(snap.active == 1, snap.idle == 0, snap.total == 1, snap.waiting == 0)
      },
      test("a parked acquire is counted separately") {
        for {
          url  <- backend.freshUrl
          snap <- ZIO.scoped {
            pool(backend.config(url).copy(maximumPoolSize = 1), recorded).flatMap { p =>
              for {
                held   <- Promise.make[Nothing, Unit]
                holder <- ZIO
                  .scoped(p.connection *> held.succeed(()) *> ZIO.sleep(80.millis))
                  .fork
                _      <- held.await
                _      <- ZIO.scoped(p.connection)
                _      <- holder.join
                snap   <- p.metrics
              } yield snap
            }
          }
        } yield assertTrue(
          snap.acquires == 2L,
          snap.parkedAcquires == 1L,
          snap.parkedNanos > 0L,
          snap.averageParkedNanos > 0.0,
        )
      } @@ withLiveClock,
      test("a timed-out acquire is counted") {
        for {
          url  <- backend.freshUrl
          snap <- ZIO.scoped {
            pool(
              backend.config(url).copy(maximumPoolSize = 1, connectionTimeout = 60.millis),
              recorded,
            ).flatMap { p =>
              ZIO.scoped(p.connection *> ZIO.scoped(p.connection).either) *> p.metrics
            }
          }
        } yield assertTrue(snap.acquireTimeouts == 1L, snap.acquires == 1L)
      } @@ withLiveClock,
      test("a retired connection is counted as retired and closed") {
        for {
          url  <- backend.freshUrl
          snap <- ZIO.scoped {
            pool(
              backend
                .config(url)
                .copy(
                  maximumPoolSize = 2,
                  minimumIdle = Some(0),
                  idleTimeout = 1.minute,
                ),
              recorded,
            ).flatMap { p =>
              ZIO.scoped(p.connection) *>
                TestClock.adjust(2.minutes) *>
                p.maintain *>
                p.metrics
            }
          }
        } yield assertTrue(
          snap.connectionsRetired == 1L,
          snap.connectionsClosed == 1L,
          snap.total == 0,
        )
      } @@ TestAspect.withLiveRandom,
      test("a suspected leak is counted") {
        for {
          url  <- backend.freshUrl
          snap <- ZIO.scoped {
            pool(
              backend.config(url).copy(leakDetectionThreshold = 1.minute),
              recorded,
            ).flatMap { p =>
              for {
                held  <- Promise.make[Nothing, Unit]
                fiber <- ZIO.scoped(p.connection *> held.succeed(()) *> ZIO.never).fork
                _     <- held.await
                _     <- TestClock.adjust(2.minutes)
                _     <- p.maintain
                snap  <- p.metrics
                _     <- fiber.interrupt
              } yield snap
            }
          }
        } yield assertTrue(snap.leaksSuspected == 1L)
      },
      test("nothing counted carries a url or a credential") {
        val snap = PoolMetrics.recording.counters
        assertTrue(
          !snap.toString.contains("jdbc:"),
          snap.toString.startsWith("PoolMetricsSnapshot"),
        )
      },
    ),
    suite("zio metrics")(
      test("pool state reaches the metric registry") {
        val hooks = PoolHooks(metrics = PoolMetrics.zio("observed", 20.millis))
        for {
          url   <- backend.freshUrl
          _     <- ZIO.scoped {
            pool(backend.config(url).copy(poolName = "observed"), hooks).flatMap { p =>
              ZIO.scoped(p.connection) *> ZIO.sleep(120.millis)
            }
          }
          state <- zio.metrics.Metric
            .gauge("zoi_pool_connections_total")
            .tagged("pool", "observed")
            .value
        } yield assertTrue(state.value >= 1.0)
      } @@ withLiveClock,
    ),
    suite("management bean")(
      test("registers while the pool lives and goes when it closes") {
        val server = ManagementFactory.getPlatformMBeanServer
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(poolName = "jmx-life", jmxEnabled = true)
          name   = PoolManagement.objectName(config)
          during <- ZIO.scoped {
            pool(config, recorded) *> ZIO.attempt(server.isRegistered(name))
          }
          after  <- ZIO.attempt(server.isRegistered(name))
        } yield assertTrue(during, !after)
      },
      test("reports the same numbers the pool reports") {
        val server = ManagementFactory.getPlatformMBeanServer
        for {
          url <- backend.freshUrl
          config = backend
            .config(url)
            .copy(
              poolName = "jmx-numbers",
              maximumPoolSize = 4,
              jmxEnabled = true,
            )
          name   = PoolManagement.objectName(config)
          result <- ZIO.scoped {
            pool(config, recorded).flatMap { p =>
              ZIO.scoped(p.connection) *> ZIO.attempt {
                (
                  server.getAttribute(name, "TotalConnections").asInstanceOf[Int],
                  server.getAttribute(name, "MaximumPoolSize").asInstanceOf[Int],
                  server.getAttribute(name, "ConnectionsCreated").asInstanceOf[Long],
                  server.getAttribute(name, "PoolName").asInstanceOf[String],
                )
              }
            }
          }
        } yield assertTrue(
          result._1 == 1,
          result._2 == 4,
          result._3 == 1L,
          result._4 == "jmx-numbers",
        )
      },
      test("suspends and resumes the pool through the bean") {
        val server = ManagementFactory.getPlatformMBeanServer
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(poolName = "jmx-suspend", jmxEnabled = true)
          name   = PoolManagement.objectName(config)
          result <- ZIO.scoped {
            pool(config, recorded).flatMap { p =>
              for {
                _         <- ZIO.attempt(server.invoke(name, "suspendPool", null, null))
                suspended <- p.state.map(_.suspended)
                _         <- ZIO.attempt(server.invoke(name, "resumePool", null, null))
                running   <- p.state.map(_.suspended)
              } yield (suspended, running)
            }
          }
        } yield assertTrue(result._1, !result._2)
      },
      test("a pool without jmx enabled registers nothing") {
        val server = ManagementFactory.getPlatformMBeanServer
        for {
          url <- backend.freshUrl
          config = backend.config(url).copy(poolName = "jmx-off")
          name   = PoolManagement.objectName(config)
          during <- ZIO.scoped(pool(config, recorded) *> ZIO.attempt(server.isRegistered(name)))
        } yield assertTrue(!during)
      },
    ),
  ) @@ withLiveRandom @@ sequential @@ timeout(120.seconds)
}
