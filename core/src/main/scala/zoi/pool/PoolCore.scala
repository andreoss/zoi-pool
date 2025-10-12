package zoi.pool

import java.sql.SQLException

import zio.stm.{STM, TRef, USTM, ZSTM}
import zio.{Chunk, Duration, IO, UIO, ZIO}

/**
 * Owns the idle resources, the size cap and the parked acquirers, and nothing
 * else: the pool's hand-off primitive, generic in the resource so it can be
 * exercised with cheap tokens as well as with real connections.
 *
 * One transactional region keeps idle, total and shutdown consistent, so a
 * hand-off cannot interleave with a reservation or a shutdown drain.
 */
private[pool] final class PoolCore[A](
  poolName: String,
  maxSize: Int,
  idleRef: TRef[List[A]],
  totalRef: TRef[Int],
  waitingRef: TRef[Int],
  closedRef: TRef[Boolean],
) {
  import PoolCore._

  /**
   * Takes an idle resource, or reserves a slot the caller must fill, or parks
   * until one of the two becomes possible.
   */
  def acquire(timeout: Duration): IO[SQLException, Acquired[A]] =
    tryAcquire.commit.uninterruptible.flatMap {
      case Some(acquired) => ZIO.succeed(acquired)
      case None           => park(timeout)
    }

  /** Reserves a slot without waiting, for prefill paths. */
  def tryReserve: UIO[Boolean] =
    ZSTM.atomically {
      for {
        closed <- closedRef.get
        total  <- totalRef.get
        can     = !closed && total < maxSize
        _      <- ZSTM.when(can)(totalRef.set(total + 1))
      } yield can
    }.uninterruptible

  /** Hands a resource to a waiting acquirer, or parks it as idle. */
  def offer(resource: A): UIO[Offered] =
    ZSTM.atomically {
      closedRef.get.flatMap {
        case true  => ZSTM.succeed(Offered.Discarded: Offered)
        case false => idleRef.update(resource :: _).as(Offered.Pooled: Offered)
      }
    }.uninterruptible

  /** Frees a slot whose resource was never created or has been destroyed. */
  def releaseSlot: UIO[Unit] =
    totalRef.update(t => if (t > 0) t - 1 else 0).commit.uninterruptible

  /** Removes one specific idle resource, reporting whether it was still there. */
  def removeIdle(resource: A): UIO[Boolean] =
    ZSTM.atomically {
      idleRef.modify { idle =>
        val without = removeFirst(idle, resource)
        (without.length != idle.length, without)
      }
    }.uninterruptible

  /** Takes every idle resource, leaving the slots reserved for the caller. */
  def drainIdle: UIO[Chunk[A]] =
    idleRef.modify(idle => (Chunk.fromIterable(idle), Nil)).commit.uninterruptible

  /** Takes up to `limit` idle resources the predicate selects, oldest first. */
  def takeIdleWhere(limit: Int, select: A => Boolean): UIO[Chunk[A]] =
    if (limit <= 0) ZIO.succeed(Chunk.empty)
    else
      ZSTM.atomically {
        idleRef.modify { idle =>
          val (taken, kept) = PoolCore.pickOldest(idle, limit, select)
          (taken, kept)
        }
      }.uninterruptible
  def idleCount: UIO[Int]    = idleRef.get.map(_.length).commit
  def totalCount: UIO[Int]   = totalRef.get.commit
  def waitingCount: UIO[Int] = waitingRef.get.commit
  def isShutdown: UIO[Boolean] = closedRef.get.commit

  /** Rejects new acquires and wakes every parked acquirer. */
  def shutdown: UIO[Unit] = closedRef.set(true).commit.uninterruptible

  private def tryAcquire: USTM[Option[Acquired[A]]] =
    closedRef.get.flatMap {
      case true  => ZSTM.succeed(None)
      case false =>
        idleRef.get.flatMap {
          case head :: tail => idleRef.set(tail).as(Some(Acquired.Ready(head, waited = false)))
          case Nil          =>
            totalRef.get.flatMap {
              case total if total < maxSize => totalRef.set(total + 1).as(Some(Acquired.Reserved))
              case _                        => ZSTM.succeed(None)
            }
        }
    }

  private def blockingAcquire: STM[SQLException, Acquired[A]] =
    closedRef.get.flatMap {
      case true  => ZSTM.fail(new PoolShutdownException(poolName))
      case false =>
        idleRef.get.flatMap {
          case head :: tail => idleRef.set(tail).as(Acquired.Ready(head, waited = true))
          case Nil          =>
            totalRef.get.flatMap {
              case total if total < maxSize => totalRef.set(total + 1).as(Acquired.Reserved)
              case _                        => ZSTM.retry
            }
        }
    }

  /**
   * Parks until a resource or a slot frees up. The outcome is published into a
   * slot inside the same transaction, so an interrupt or a timeout that lands
   * after the commit still finds what was taken and gives it back.
   *
   * Only the commit is interruptible: the pool acquires inside an uninterruptible
   * region, and a park that could not be interrupted could not be timed out.
   */
  private def park(timeout: Duration): IO[SQLException, Acquired[A]] =
    TRef.makeCommit(Option.empty[Acquired[A]]).flatMap { slot =>
      ZIO.acquireReleaseWith(enterWait)(_ => leaveWait) { _ =>
        blockingAcquire
          .flatMap(acquired => slot.set(Some(acquired)).as(acquired))
          .commit
          .interruptible
          .onInterrupt(restore(slot))
          .timeout(timeout)
          .flatMap {
            case Some(acquired) => ZIO.succeed(acquired)
            case None           =>
              restore(slot) *> ZIO.fail(new PoolTimeoutException(poolName, timeout))
          }
      }
    }

  private def restore(slot: TRef[Option[Acquired[A]]]): UIO[Unit] =
    slot.getAndSet(None).commit.flatMap {
      case Some(Acquired.Ready(resource, _)) => offer(resource).unit
      case Some(Acquired.Reserved)           => releaseSlot
      case None                              => ZIO.unit
    }

  private def enterWait: UIO[Unit] = waitingRef.update(_ + 1).commit.uninterruptible
  private def leaveWait: UIO[Unit] =
    waitingRef.update(w => if (w > 0) w - 1 else 0).commit.uninterruptible
}

private[pool] object PoolCore {

  /** What an acquire produced: a ready resource, or a slot to fill. */
  sealed trait Acquired[+A]
  object Acquired {
    case object Reserved                                 extends Acquired[Nothing]
    final case class Ready[A](resource: A, waited: Boolean) extends Acquired[A]
  }

  /** What an offer did with a returned resource. */
  sealed trait Offered
  object Offered {
    case object Pooled    extends Offered
    case object Discarded extends Offered
  }

  def make[A](poolName: String, maxSize: Int): UIO[PoolCore[A]] =
    ZSTM.atomically {
      for {
        idle    <- TRef.make(List.empty[A])
        total   <- TRef.make(0)
        waiting <- TRef.make(0)
        closed  <- TRef.make(false)
      } yield new PoolCore[A](poolName, maxSize, idle, total, waiting, closed)
    }

  private[pool] def pickOldest[A](
    idle: List[A],
    limit: Int,
    select: A => Boolean,
  ): (Chunk[A], List[A]) = {
    val builder = Chunk.newBuilder[A]
    var budget  = limit
    val kept    = idle.reverse.filter { candidate =>
      if (budget > 0 && select(candidate)) {
        builder += candidate
        budget -= 1
        false
      } else true
    }
    (builder.result(), kept.reverse)
  }

  private def removeFirst[A](list: List[A], value: A): List[A] = {
    val index = list.indexWhere(_.asInstanceOf[AnyRef] eq value.asInstanceOf[AnyRef])
    if (index < 0) list else list.take(index) ::: list.drop(index + 1)
  }
}
