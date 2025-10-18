package zoi.pool.bench

import java.sql.Connection

import zio.{Task, ZIO}

/** One unit of work, run over and over against a borrow. */
final case class Workload(name: String, fibers: Int, run: BenchPool.Borrow => Task[Unit])

object Workload {

  private val table = "zoi_bench"

  def all(settings: BenchSettings): List[Workload] =
    List(
      Workload("sequential", 1, borrow => borrow(_ => ()).unit),
      Workload("parallel", settings.parallelFibers, borrow => borrow(_ => ()).unit),
      Workload("mixed", 1, borrow => borrow(selectOne).unit),
      Workload("prepared", 1, borrow => borrow(preparedSelect).unit),
      Workload("statement", 1, borrow => borrow(prepareAndClose).unit),
      Workload("transaction", 1, borrow => borrow(transaction).unit),
      Workload("saturated", settings.saturationFibers, borrow => borrow(selectOne).unit),
    )

  def selected(settings: BenchSettings): List[Workload] = {
    val everything = all(settings)
    if (settings.workloads.contains("all")) everything
    else everything.filter(workload => settings.workloads.contains(workload.name))
  }

  /** Makes sure the table the query workloads need exists. */
  def prepare(borrow: BenchPool.Borrow): Task[Unit] =
    borrow { connection =>
      val statement = connection.createStatement()
      try {
        statement.execute(s"CREATE TABLE IF NOT EXISTS $table (id INT PRIMARY KEY, name VARCHAR(32))")
        statement.execute(s"DELETE FROM $table")
        statement.execute(s"INSERT INTO $table (id, name) VALUES (1, 'one')")
      } finally statement.close()
    }.unit

  private def selectOne(connection: Connection): Unit = {
    val statement = connection.createStatement()
    try {
      val results = statement.executeQuery("SELECT 1")
      try results.next()
      finally results.close()
      ()
    } finally statement.close()
  }

  private def preparedSelect(connection: Connection): Unit = {
    val statement = connection.prepareStatement(s"SELECT name FROM $table WHERE id = ?")
    try {
      statement.setInt(1, 1)
      val results = statement.executeQuery()
      try results.next()
      finally results.close()
      ()
    } finally statement.close()
  }

  private def prepareAndClose(connection: Connection): Unit = {
    val statement = connection.prepareStatement(s"SELECT name FROM $table WHERE id = ?")
    statement.close()
  }

  private def transaction(connection: Connection): Unit = {
    val restore = connection.getAutoCommit
    connection.setAutoCommit(false)
    try {
      selectOne(connection)
      connection.commit()
    } finally connection.setAutoCommit(restore)
  }

  /** Runs one workload `count` times, spread over its fibers. */
  def repeat(workload: Workload, borrow: BenchPool.Borrow, count: Int): Task[Unit] =
    if (workload.fibers <= 1) ZIO.foreachDiscard(1 to count)(_ => workload.run(borrow))
    else {
      val perFiber = math.max(1, count / workload.fibers)
      ZIO.foreachParDiscard(1 to workload.fibers) { _ =>
        ZIO.foreachDiscard(1 to perFiber)(_ => workload.run(borrow))
      }
    }
}
