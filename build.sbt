val zioVersion            = "2.1.22"
val h2Version             = "2.3.232"
val derbyVersion          = "10.17.1.0"
val sqliteVersion         = "3.47.1.0"
val zioJdbcVersion        = "0.1.2"
val quillVersion          = "4.8.5"
val doobieVersion         = "1.0.0-RC11"
val testcontainersVersion = "1.21.3"
val postgresVersion       = "42.7.8"
val mysqlVersion          = "9.1.0"
val mariadbVersion        = "3.5.1"
val oracleVersion         = "23.9.0.25.07"
val mssqlVersion          = "12.10.1.jre11"
val slf4jVersion          = "2.0.16"
val scala3Version         = "3.3.7"
val scala213Version       = "2.13.17"

inThisBuild(
  List(
    organization       := "dev.zoi",
    version            := sys.env.getOrElse("ZOI_VERSION", "0.1.0-SNAPSHOT"),
    scalaVersion       := scala3Version,
    crossScalaVersions := List(scala213Version, scala3Version),
    semanticdbEnabled  := false,
    versionScheme      := Some("early-semver"),
    homepage           := Some(url("https://github.com/zoi-pool/zoi-pool")),
    developers         := List(
      Developer("zoi-pool", "zoi-pool", "", url("https://github.com/zoi-pool")),
    ),
    semanticdbVersion  := scalafixSemanticdb.revision,
    licenses           := List("LGPL-3.0" -> url("https://www.gnu.org/licenses/lgpl-3.0.html")),
    scmInfo            := Some(
      ScmInfo(
        url("https://github.com/zoi-pool/zoi-pool"),
        "scm:git:https://github.com/zoi-pool/zoi-pool.git",
      ),
    ),
    credentials ++= sys.env
      .get("SONATYPE_USERNAME")
      .zip(sys.env.get("SONATYPE_PASSWORD"))
      .map { case (user, password) =>
        Credentials(
          "Sonatype Nexus Repository Manager",
          xerial.sbt.Sonatype.sonatypeCentralHost,
          user,
          password,
        )
      }
      .toList,
  ),
)

val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Werror"),
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq("-source:3.0-migration")
    case _            => Seq("-Xsource:3", "-Wconf:cat=scala3-migration:s")
  }),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  // Forked so JDBC driver discovery sees a flat classpath: DriverManager only
  // offers a driver the calling classloader can also load by name.
  Test / fork := true,
  Test / javaOptions += "-Dderby.stream.error.file=target/derby.log",
)

// Published modules keep binary compatibility within a release line, and hold
// the coverage gate the process asks for.
val releaseSettings = Seq(
  coverageMinimumStmtTotal   := 85,
  coverageMinimumBranchTotal := 70,
  coverageFailOnMinimum      := true,
  // Set to the previous release once one exists; empty means nothing to check.
  mimaPreviousArtifacts      := Set.empty,
  publishMavenStyle          := true,
  sonatypeCredentialHost     := xerial.sbt.Sonatype.sonatypeCentralHost,
  publishTo                  := sonatypePublishToBundle.value,
  pgpPassphrase              := sys.env.get("PGP_PASSPHRASE").map(_.toArray),
)
lazy val root       = (project in file("."))
  .disablePlugins(MimaPlugin)
  .aggregate(core, interop, consumers, examples)
  .settings(
    name           := "zoi-pool-root",
    publish / skip := true,
  )

lazy val core = (project in file("core"))
  .settings(releaseSettings)
  .settings(commonSettings)
  .settings(
    name := "zoi-pool",
    libraryDependencies ++= Seq(
      "dev.zio"                 %% "zio"                 % zioVersion,
      "dev.zio"                 %% "zio-test"            % zioVersion            % Test,
      "dev.zio"                 %% "zio-test-sbt"        % zioVersion            % Test,
      "com.h2database"           % "h2"                  % h2Version             % Test,
      "org.apache.derby"         % "derby"               % derbyVersion          % Test,
      "org.apache.derby"         % "derbytools"          % derbyVersion          % Test,
      "org.xerial"               % "sqlite-jdbc"         % sqliteVersion         % Test,
      "org.testcontainers"       % "postgresql"          % testcontainersVersion % Test,
      "org.testcontainers"       % "mysql"               % testcontainersVersion % Test,
      "org.testcontainers"       % "mariadb"             % testcontainersVersion % Test,
      "org.testcontainers"       % "oracle-xe"           % testcontainersVersion % Test,
      "org.testcontainers"       % "mssqlserver"         % testcontainersVersion % Test,
      "org.postgresql"           % "postgresql"          % postgresVersion       % Test,
      "com.mysql"                % "mysql-connector-j"   % mysqlVersion          % Test,
      "org.mariadb.jdbc"         % "mariadb-java-client" % mariadbVersion        % Test,
      "com.oracle.database.jdbc" % "ojdbc11"             % oracleVersion         % Test,
      "com.microsoft.sqlserver"  % "mssql-jdbc"          % mssqlVersion          % Test,
      "org.slf4j"                % "slf4j-simple"        % slf4jVersion          % Test,
    ),
  )

// Performance harness. Its own project so the pools and JDBC drivers it
// measures against never reach the library's classpath.
// Run: `sbt "bench/run"`, configured by BENCH_* environment variables.
lazy val bench = (project in file("bench"))
  .disablePlugins(MimaPlugin)
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name                      := "zoi-pool-bench",
    publish / skip            := true,
    Compile / run / mainClass := Some("zoi.pool.bench.BenchMain"),
    run / fork                := true,
    run / connectInput        := true,
    run / baseDirectory       := (LocalRootProject / baseDirectory).value,
    libraryDependencies ++= Seq(
      "dev.zio"       %% "zio"               % zioVersion,
      "dev.zio"       %% "zio-test"          % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt"      % zioVersion % Test,
      "com.zaxxer"     % "HikariCP"          % "5.1.0",
      "com.h2database" % "h2"                % h2Version,
      "org.postgresql" % "postgresql"        % "42.7.8",
      "com.mysql"      % "mysql-connector-j" % "9.1.0",
      "org.slf4j"      % "slf4j-simple"      % "2.0.16",
    ),
  )

// Adapters for consumers that take something other than a DataSource.
lazy val interop = (project in file("interop"))
  .settings(releaseSettings)
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    name := "zoi-pool-interop",
    libraryDependencies ++= Seq(
      "dev.zio"       %% "zio-jdbc"     % zioJdbcVersion,
      "dev.zio"       %% "zio-test"     % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt" % zioVersion % Test,
      "com.h2database" % "h2"           % h2Version  % Test,
    ),
  )

// Proof that consumers which only know JDBC can use the pool. Test only, so
// their dependencies never reach the published artifacts.
lazy val consumers = (project in file("consumers"))
  .disablePlugins(MimaPlugin)
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    name           := "zoi-pool-consumers",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "io.getquill"   %% "quill-jdbc-zio" % quillVersion  % Test,
      "org.tpolecat"  %% "doobie-core"    % doobieVersion % Test,
      "dev.zio"       %% "zio-test"       % zioVersion    % Test,
      "dev.zio"       %% "zio-test-sbt"   % zioVersion    % Test,
      "com.h2database" % "h2"             % h2Version     % Test,
    ),
  )

// Runnable examples. Built in CI so they cannot rot, never published.
lazy val examples = (project in file("examples"))
  .disablePlugins(MimaPlugin)
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name           := "zoi-pool-examples",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "com.h2database" % "h2" % h2Version,
    ),
  )
