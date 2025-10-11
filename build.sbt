val zioVersion      = "2.1.21"
val scala3Version   = "3.3.7"
val scala213Version = "2.13.17"

inThisBuild(
  List(
    organization       := "dev.zoi",
    version            := "0.1.0-SNAPSHOT",
    scalaVersion       := scala3Version,
    crossScalaVersions := List(scala213Version, scala3Version),
    semanticdbEnabled  := true,
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
  .aggregate(core)
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
    ),
  )
