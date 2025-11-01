package zoi.pool.bench

import zio.{Duration, durationInt}

/** Everything a run needs, read from BENCH_* variables or system properties. */
final case class BenchSettings(
  database: String,
  url: String,
  username: Option[String],
  password: Option[String],
  pools: List[String],
  workloads: List[String],
  iterations: Int,
  warmup: Int,
  rounds: Int,
  forks: Int,
  maximumPoolSize: Int,
  minimumIdle: Int,
  parallelFibers: Int,
  saturationFibers: Int,
  mode: String,
  output: String,
  baseline: Option[String],
  tolerance: Double,
  connectionTimeout: Duration,
)

object BenchSettings {

  private val embedded = Set("h2", "derby")

  def load(args: List[String]): BenchSettings = {
    val database = read("database").getOrElse("h2").toLowerCase
    val url      = read("url").getOrElse(defaultUrl(database))
    val heavy    = !embedded.contains(database)

    BenchSettings(
      database = database,
      url = url,
      username = read("username").orElse(defaultUser(database)),
      password = read("password").orElse(defaultUser(database)),
      pools = if (args.nonEmpty) args else list("pools", "zoi,hikari,none"),
      workloads = list("workloads", "all"),
      iterations = int("iterations", if (heavy) 2000 else 20000),
      warmup = int("warmup", if (heavy) 200 else 2000),
      rounds = int("rounds", 5),
      forks = int("forks", 1),
      maximumPoolSize = int("maxPoolSize", 10),
      minimumIdle = int("minIdle", 2),
      parallelFibers = int("parallelFibers", 10),
      saturationFibers = int("saturationFibers", 50),
      mode = read("mode").getOrElse("throughput").toLowerCase,
      output = read("output").getOrElse("table").toLowerCase,
      baseline = read("baseline"),
      tolerance = read("tolerance").map(_.toDouble).getOrElse(0.25),
      connectionTimeout = 30.seconds,
    )
  }

  private def defaultUrl(database: String): String = database match {
    case "postgres" | "psql" => "jdbc:postgresql://localhost:5432/bench"
    case "mysql"             => "jdbc:mysql://localhost:3306/bench?useSSL=false&allowPublicKeyRetrieval=true"
    case "derby"             => "jdbc:derby:memory:bench;create=true"
    case _                   => "jdbc:h2:mem:bench;DB_CLOSE_DELAY=-1"
  }

  private def defaultUser(database: String): Option[String] = database match {
    case "postgres" | "psql" | "mysql" => Some("bench")
    case _                             => None
  }

  private def read(key: String): Option[String] =
    sys.env
      .get("BENCH_" + key.toUpperCase)
      .orElse(sys.props.get("bench." + key))
      .map(_.trim)
      .filter(_.nonEmpty)

  private def list(key: String, fallback: String): List[String] =
    read(key).getOrElse(fallback).split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toList

  private def int(key: String, fallback: Int): Int = read(key).map(_.toInt).getOrElse(fallback)
}
