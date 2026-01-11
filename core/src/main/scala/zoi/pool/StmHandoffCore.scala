package zoi.pool

import java.sql.SQLException

import zio.stm.{STM, TRef, USTM, ZSTM}
import zio.{Chunk, Duration, IO, UIO, ZIO}

import zoi.pool.HandoffCore.{Acquired, Offered}

/**
 * A hand-off core that keeps idle, total and shutdown inside one transactional
 * region, so it is race-free by construction.
 *
 * It is the model the shipped core is checked against: slower, but obviously
 * correct, which is what a reference implementation is for.
 */
final private[pool] class StmHandoffCore[A](
    poolName: String,
    maxSize: Int,
    idleRef: TRef[List[A]],
    totalRef: TRef[Int],
    waitingRef: TRef[Int],
    closedRef: TRef[Boolean],
) extends HandoffCore[A] {

  def acquire(timeout: Duration): IO[SQLException, Acquired[A]] =
    tryAcquire.commit.uninterruptible.flatMap {
      case Some(acquired) => ZIO.succeed(acquired)
      case None           => park(timeout)
    }

  def tryReserve: UIO[Boolean] =
    ZSTM.atomically {
      for {
        closed <- closedRef.get
        total  <- totalRef.get
        can = !closed && total < maxSize
        _ <- ZSTM.when(can)(totalRef.set(total + 1))
      } yield can
    }.uninterruptible

  def offer(resource: A): UIO[Offered] =
    ZSTM.atomically {
      closedRef.get.flatMap {
        case true  => ZSTM.succeed(Offered.Discarded: Offered)
        case false => idleRef.update(resource :: _).as(Offered.Pooled: Offered)
      }
    }.uninterruptible

  def releaseSlot: UIO[Unit] =
    totalRef.update(t => if (t > 0) t - 1 else 0).commit.uninterruptible

  def drainIdle: UIO[Chunk[A]] =
    idleRef.modify(idle => (Chunk.fromIterable(idle), Nil)).commit.uninterruptible

  def takeIdleWhere(limit: Int, select: A => Boolean): UIO[Chunk[A]] =
    if (limit <= 0) ZIO.succeed(Chunk.empty)
    else
      ZSTM.atomically {
        idleRef.modify(idle => StmHandoffCore.pickOldest(idle, limit, select))
      }.uninterruptible

  def idleCount: UIO[Int]      = idleRef.get.map(_.length).commit
  def totalCount: UIO[Int]     = totalRef.get.commit
  def waitingCount: UIO[Int]   = waitingRef.get.commit
  def isShutdown: UIO[Boolean] = closedRef.get.commit

  def shutdown: UIO[Unit] = closedRef.set(true).commit.uninterruptible

  private def tryAcquire: USTM[Option[Acquired[A]]] =
    closedRef.get.flatMap {
      case true  => ZSTM.succeed(None)
      case false =>
        idleRef.get.flatMap {
          case head :: tail =>
            idleRef.set(tail).as(Some(Acquired.Ready(head, waited = false)))
          case Nil          =>
            totalRef.get.flatMap {
              case total if total < maxSize =>
                totalRef.set(total + 1).as(Some(Acquired.Reserved(waited = false)))
              case _                        => ZSTM.succeed(None)
            }
        }
    }

  private def blockingAcquire: STM[SQLException, Acquired[A]] =
    closedRef.get.flatMap {
      case true  => ZSTM.fail(HandoffCore.shutdownFailure(poolName))
      case false =>
        idleRef.get.flatMap {
          case head :: tail => idleRef.set(tail).as(Acquired.Ready(head, waited = true))
          case Nil          =>
            totalRef.get.flatMap {
              case total if total < maxSize =>
                totalRef.set(total + 1).as(Acquired.Reserved(waited = true))
              case _                        => ZSTM.retry
            }
        }
    }

  /**
   * Parks until a resource or a slot frees up. The outcome is published into a
   * slot inside the same transaction, so an interrupt or a timeout that lands
   * after the commit still finds what was taken and gives it back.
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
      case Some(Acquired.Reserved(_))        => releaseSlot
      case None                              => ZIO.unit
    }

  private def enterWait: UIO[Unit] = waitingRef.update(_ + 1).commit.uninterruptible

  private def leaveWait: UIO[Unit] =
    waitingRef.update(w => if (w > 0) w - 1 else 0).commit.uninterruptible
}

private[pool] object StmHandoffCore {

  def make[A](poolName: String, maxSize: Int): UIO[HandoffCore[A]] =
    ZSTM.atomically {
      for {
        idle    <- TRef.make(List.empty[A])
        total   <- TRef.make(0)
        waiting <- TRef.make(0)
        closed  <- TRef.make(false)
      } yield new StmHandoffCore[A](poolName, maxSize, idle, total, waiting, closed)
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
}
