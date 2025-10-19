package zoi.pool

import java.sql.SQLException

import zio.{Chunk, Duration, IO, Scope, UIO, ZIO}

/**
 * The pool's hand-off primitive: it owns the idle resources, the size cap and
 * the parked acquirers, and nothing else.
 *
 * Generic in the resource so the hand-off can be exercised with cheap tokens as
 * well as with real connections, and behind an interface so an implementation
 * can be replaced and compared without touching the pool body.
 */
private[pool] trait HandoffCore[A] {

  /** Takes an idle resource, reserves a slot to fill, or parks until either. */
  def acquire(timeout: Duration): IO[SQLException, HandoffCore.Acquired[A]]

  /** Reserves a slot without waiting, for prefill paths. */
  def tryReserve: UIO[Boolean]

  /** Hands a resource to a waiting acquirer, or parks it as idle. */
  def offer(resource: A): UIO[HandoffCore.Offered]

  /** Frees a slot whose resource was never created or has been destroyed. */
  def releaseSlot: UIO[Unit]

  /** Removes one specific idle resource, reporting whether it was still there. */
  def removeIdle(resource: A): UIO[Boolean]

  /** Takes every idle resource, leaving the slots reserved for the caller. */
  def drainIdle: UIO[Chunk[A]]

  /** Takes up to `limit` idle resources the predicate selects, oldest first. */
  def takeIdleWhere(limit: Int, select: A => Boolean): UIO[Chunk[A]]

  def idleCount: UIO[Int]
  def totalCount: UIO[Int]
  def waitingCount: UIO[Int]
  def isShutdown: UIO[Boolean]

  /** Rejects new acquires and wakes every parked acquirer. */
  def shutdown: UIO[Unit]
}

private[pool] object HandoffCore {

  /** What an acquire produced: a ready resource, or a slot to fill. */
  sealed trait Acquired[+A]
  object Acquired {
    final case class Reserved(waited: Boolean)              extends Acquired[Nothing]
    final case class Ready[A](resource: A, waited: Boolean) extends Acquired[A]
  }

  /** What an offer did with a returned resource. */
  sealed trait Offered
  object Offered {
    case object Pooled    extends Offered
    case object Discarded extends Offered
  }

  def shutdownFailure(poolName: String): SQLException = new PoolShutdownException(poolName)

  /** The implementation the pool ships with. */
  def make[A](poolName: String, maxSize: Int): ZIO[Scope, Nothing, HandoffCore[A]] =
    LockFreeHandoffCore.make[A](poolName, maxSize)
}
