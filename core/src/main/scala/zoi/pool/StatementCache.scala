package zoi.pool

import java.sql.{Connection, PreparedStatement, SQLException}

import scala.collection.mutable

/**
 * Per-connection prepared-statement cache with least-recently-used eviction.
 *
 * A connection is used by one borrower at a time, so the cache needs no
 * concurrency beyond the monitor that guards its own map.
 */
final private[pool] class StatementCache(connection: Connection, capacity: Int) {

  private val entries = mutable.LinkedHashMap.empty[String, PreparedStatement]

  def prepare(sql: String): PreparedStatement =
    synchronized {
      entries.remove(sql) match {
        case Some(cached) if !cached.isClosed =>
          entries.put(sql, cached)
          cached.clearParameters()
          cached
        case _                                =>
          val prepared = connection.prepareStatement(sql)
          evictIfFull()
          entries.put(sql, prepared)
          prepared
      }
    }

  def size: Int = synchronized(entries.size)

  def close(): Unit =
    synchronized {
      entries.values.foreach(closeQuietly)
      entries.clear()
    }

  private def evictIfFull(): Unit =
    if (entries.size >= capacity) {
      entries.headOption.foreach { case (sql, statement) =>
        entries.remove(sql)
        closeQuietly(statement)
      }
    }

  private def closeQuietly(statement: PreparedStatement): Unit =
    try statement.close()
    catch { case _: SQLException => () }
}
