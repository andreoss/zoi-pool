package zoi.pool.bench

import java.nio.file.Files

import zio.ZIO
import zio.test._

/** The regression gate has to fire on the library and on nothing else. */
object ReportSpec extends ZIOSpecDefault {

  private def baseline(zoi: Double, hikari: Double): ZIO[Any, Throwable, String] =
    ZIO.attempt {
      val file = Files.createTempFile("baseline", ".csv")
      val rows = List(
        "database,workload,pool,ops_per_second,lowest,highest,p50_micros,p99_micros,p999_micros",
        s"h2,statement,zoi,$zoi,0,0,0,0,0",
        s"h2,statement,hikari,$hikari,0,0,0,0,0",
      )
      Files.write(file, java.util.Arrays.asList(rows: _*))
      file.toFile.deleteOnExit()
      file.toString
    }

  private def settings(path: String): BenchSettings =
    BenchSettings.load(Nil).copy(baseline = Some(path), tolerance = 0.25)

  private def run(zoi: Double, hikari: Double): List[Measurement] =
    List(
      Measurement("h2", "statement", "zoi", zoi, zoi, zoi, 0.0, 0.0, 0.0),
      Measurement("h2", "statement", "hikari", hikari, hikari, hikari, 0.0, 0.0, 0.0),
    )

  def spec = suite("regression gate")(
    test("a steady run does not regress") {
      for {
        path <- baseline(zoi = 100.0, hikari = 200.0)
        found = Report.regressions(settings(path), run(zoi = 100.0, hikari = 200.0))
      } yield assertTrue(found.isEmpty)
    },
    test("the incumbent pulling ahead on its own is not a regression") {
      for {
        path <- baseline(zoi = 100.0, hikari = 200.0)
        found = Report.regressions(settings(path), run(zoi = 120.0, hikari = 600.0))
      } yield assertTrue(found.isEmpty)
    },
    test("the library falling behind past the tolerance regresses") {
      for {
        path <- baseline(zoi = 100.0, hikari = 200.0)
        found = Report.regressions(settings(path), run(zoi = 50.0, hikari = 200.0))
      } yield assertTrue(found.size == 1, found.head.startsWith("statement/zoi"))
    },
    test("a uniformly slower machine does not regress") {
      for {
        path <- baseline(zoi = 100.0, hikari = 200.0)
        found = Report.regressions(settings(path), run(zoi = 50.0, hikari = 100.0))
      } yield assertTrue(found.isEmpty)
    },
  )
}
