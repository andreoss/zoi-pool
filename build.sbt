val zioVersion      = "2.1.22"
val h2Version       = "2.3.232"
val derbyVersion    = "10.17.1.0"
val sqliteVersion   = "3.47.1.0"
val zioJdbcVersion  = "0.1.2"
val quillVersion    = "4.8.6"
val doobieVersion   = "1.0.0-RC10"
val scala3Version   = "3.3.7"
val scala213Version = "2.13.17"

inThisBuild(
  List(
    organization       := "dev.zoi",
    version            := "0.1.0-SNAPSHOT",
    scalaVersion       := scala3Version,
    crossScalaVersions := List(scala213Version, scala3Version),
    semanticdbEnabled  := false,
    semanticdbVersion  := scalafixSemanticdb.revision,
    licenses           := List("LGPL-3.0" -> url("https://www.gnu.org/licenses/lgpl-3.0.html")),
  ),
)

val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Werror"),
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq("-source:3.0-migration")
    case _            => Seq("-Xsource:3", "-Wconf:cat=scala3-migration:s")
  }),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
)

lazy val root = (project in file("."))
  .aggregate(core, interop)
  .settings(
    name           := "zoi-pool-root",
    publish / skip := true,
  )

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "zoi-pool",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      "com.h2database"   % "h2"          % h2Version     % Test,
      "org.apache.derby" % "derby"       % derbyVersion  % Test,
      "org.apache.derby" % "derbytools"  % derbyVersion  % Test,
      "org.xerial"       % "sqlite-jdbc" % sqliteVersion % Test,
    ),
  )

// Performance harness. Its own project so the pools and JDBC drivers it
// measures against never reach the library's classpath.
// Run: `sbt "bench/run"`, configured by BENCH_* environment variables.
lazy val bench = (project in file("bench"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name                     := "zoi-pool-bench",
    publish / skip           := true,
    Compile / run / mainClass := Some("zoi.pool.bench.BenchMain"),
    libraryDependencies ++= Seq(
      "dev.zio"    %% "zio"                % zioVersion,
      "com.zaxxer"  % "HikariCP"           % "5.1.0",
      "com.h2database" % "h2"              % h2Version,
      "org.postgresql" % "postgresql"      % "42.7.8",
      "com.mysql"   % "mysql-connector-j"  % "9.1.0",
      "org.slf4j"   % "slf4j-simple"       % "2.0.16",
    ),
  )

// Adapters for consumers that take something other than a DataSource.
lazy val interop = (project in file("interop"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    name := "zoi-pool-interop",
    libraryDependencies ++= Seq(
      "dev.zio"        %% "zio-jdbc"     % zioJdbcVersion,
      "dev.zio"        %% "zio-test"     % zioVersion % Test,
      "dev.zio"        %% "zio-test-sbt" % zioVersion % Test,
      "com.h2database"  % "h2"           % h2Version  % Test,
    ),
  )

// Proof that consumers which only know JDBC can use the pool. Test only, so
// their dependencies never reach the published artifacts.
lazy val consumers = (project in file("consumers"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    name           := "zoi-pool-consumers",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "io.getquill"    %% "quill-jdbc-zio" % quillVersion  % Test,
      "org.tpolecat"   %% "doobie-core"    % doobieVersion % Test,
      "dev.zio"        %% "zio-test"       % zioVersion    % Test,
      "dev.zio"        %% "zio-test-sbt"   % zioVersion    % Test,
      "com.h2database"  % "h2"             % h2Version     % Test,
    ),
  )
