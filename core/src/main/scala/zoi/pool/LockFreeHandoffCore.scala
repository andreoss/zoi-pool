package zoi.pool

import java.sql.SQLException
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.jdk.CollectionConverters._

import zio.{Chunk, Clock, Duration, IO, Promise, Scope, UIO, ZIO}

import zoi.pool.HandoffCore.{Acquired, Offered}

/**
 * The shipped hand-off core: an uncontended acquire or release is a deque
 * operation and a compare-and-set, with no transaction and no fibre parking.
 *
 * When the pool is at its cap the acquirer parks on a promise instead, and the
 * two paths are joined by double-checking on both sides: a returning connection
 * is published to the idle deque and only then are waiters looked at, while an
 * acquirer registers itself and only then re-reads the deque. Neither side can
 * therefore miss the other, and every hand-off is claimed exactly once.
 *
 * This is the one place the pool reaches for JDK concurrency primitives: the
 * measured cost of routing every acquire through a transaction is what
 * justifies it.
 */
final private[pool] class LockFreeHandoffCore[A](poolName: String, maxSize: Int)
    extends HandoffCore[A] {

  private val idle    = new ConcurrentLinkedDeque[A]
  private val waiters = new ConcurrentLinkedDeque[LockFreeHandoffCore.Waiter[A]]
  private val total   = new AtomicInteger(0)
  private val waiting = new AtomicInteger(0)
  private val nextDue = new java.util.concurrent.atomic.AtomicLong(Long.MaxValue)
  private val closed  = new AtomicBoolean(false)

  def acquire(timeout: Duration): IO[SQLException, Acquired[A]] =
    ZIO.suspendSucceed {
      fastAcquire() match {
        case null     => park(timeout)
        case acquired => ZIO.succeed(acquired)
      }
    }

  def tryReserve: UIO[Boolean] = ZIO.succeed(!closed.get() && reserve())

  def offer(resource: A): UIO[Offered] =
    ZIO.suspendSucceed {
      if (closed.get()) ZIO.succeed(Offered.Discarded)
      else
        handDirectly(resource) match {
          case null   =>
            idle.addFirst(resource)
            if (closed.get() && idle.remove(resource)) ZIO.succeed(Offered.Discarded)
            else complete(pairWaiters()).as(Offered.Pooled)
          case waiter =>
            waiter.promise.succeed(Acquired.Ready(resource, waited = true)).as(Offered.Pooled)
        }
    }

  /** Hands a returned resource straight to a waiter, without touching the deque. */
  private def handDirectly(resource: A): LockFreeHandoffCore.Waiter[A] =
    if (waiting.get() <= 0) null
    else {
      val waiter = takeWaiter()
      if (waiter == null) null
      else {
        waiting.decrementAndGet()
        waiter
      }
    }
  def releaseSlot: UIO[Unit]                                           =
    ZIO.suspendSucceed {
      total.decrementAndGet()
      complete(pairWaiters())
    }

  def removeIdle(resource: A): UIO[Boolean] = ZIO.succeed(idle.remove(resource))

  def drainIdle: UIO[Chunk[A]] =
    ZIO.succeed {
      val taken = Chunk.newBuilder[A]
      var next  = idle.pollFirst()
      while (next != null) {
        taken += next
        next = idle.pollFirst()
      }
      taken.result()
    }

  def takeIdleWhere(limit: Int, select: A => Boolean): UIO[Chunk[A]] =
    if (limit <= 0) ZIO.succeed(Chunk.empty)
    else
      ZIO.succeed {
        val taken    = Chunk.newBuilder[A]
        var budget   = limit
        val iterator = idle.descendingIterator()
        while (budget > 0 && iterator.hasNext) {
          val candidate = iterator.next()
          if (select(candidate) && idle.remove(candidate)) {
            taken += candidate
            budget -= 1
          }
        }
        taken.result()
      }

  def idleCount: UIO[Int]      = ZIO.succeed(idle.size())
  def totalCount: UIO[Int]     = ZIO.succeed(math.max(total.get(), 0))
  def waitingCount: UIO[Int]   = ZIO.succeed(math.max(waiting.get(), 0))
  def isShutdown: UIO[Boolean] = ZIO.succeed(closed.get())

  def shutdown: UIO[Unit] =
    ZIO.suspendSucceed {
      closed.set(true)
      val stranded = List.newBuilder[LockFreeHandoffCore.Waiter[A]]
      var next     = waiters.poll()
      while (next != null) {
        if (next.claim()) {
          waiting.decrementAndGet()
          stranded += next
        }
        next = waiters.poll()
      }
      ZIO.foreachDiscard(stranded.result())(
        _.promise.fail(HandoffCore.shutdownFailure(poolName)),
      )
    }

  /** One deque poll, or one compare-and-set against the cap. Nothing else. */
  private def fastAcquire(): Acquired[A] =
    if (closed.get()) null
    else {
      val resource = idle.pollFirst()
      if (resource != null) Acquired.Ready(resource, waited = false)
      else if (reserve()) Acquired.Reserved(waited = false)
      else null
    }

  private def reserve(): Boolean = {
    var current = total.get()
    while (current < maxSize) {
      if (total.compareAndSet(current, current + 1)) return true
      current = total.get()
    }
    false
  }

  private def park(timeout: Duration): IO[SQLException, Acquired[A]] =
    Clock.nanoTime.flatMap { now =>
      Promise.make[SQLException, Acquired[A]].flatMap { promise =>
        val waiter = new LockFreeHandoffCore.Waiter[A](promise, now + timeout.toNanos, timeout)
        ZIO.suspendSucceed {
          waiting.incrementAndGet()
          recordDeadline(waiter.deadlineNanos)
          waiters.addLast(waiter)
          register(waiter)
        }
      }
    }

  /**
   * Having registered, the acquirer looks again: anything published between its
   * first miss and its registration is picked up here rather than slept through.
   *
   * The wait itself carries no timer. One reaper per pool expires every parked
   * acquirer instead, which is what makes a saturated pool cheap: a fibre is
   * scheduled per pool, not per borrower.
   */
  private def register(waiter: LockFreeHandoffCore.Waiter[A]): IO[SQLException, Acquired[A]] = {
    val rescued = fastAcquire()
    if (rescued != null) {
      if (claimSelf(waiter)) ZIO.succeed(rescued)
      else giveBack(rescued) *> waiter.promise.await
    } else if (closed.get()) {
      if (claimSelf(waiter)) ZIO.fail(HandoffCore.shutdownFailure(poolName))
      else waiter.promise.await
    } else waiter.promise.await.interruptible.onInterrupt(abandon(waiter))
  }

  /**
   * Expires parked acquirers whose budget ran out, and drops the entries of
   * those that already left. It sleeps when nothing is waiting, so an idle pool
   * costs one sleeping fibre.
   */
  private[pool] def reaper: UIO[Unit] = {
    def loop: UIO[Unit] =
      ZIO.suspendSucceed {
        val pause =
          if (waiting.get() <= 0) LockFreeHandoffCore.QuietPause
          else LockFreeHandoffCore.BusyPause
        ZIO.sleep(pause) *> sweep *> loop
      }
    loop
  }

  /** Remembers the earliest budget, so a sweep that cannot find work is skipped. */
  private def recordDeadline(deadlineNanos: Long): Unit = {
    var current = nextDue.get()
    while (deadlineNanos < current && !nextDue.compareAndSet(current, deadlineNanos))
      current = nextDue.get()
  }

  private def sweep: UIO[Unit] =
    ZIO.suspendSucceed {
      if (waiters.isEmpty) {
        nextDue.set(Long.MaxValue)
        ZIO.unit
      } else
        Clock.nanoTime.flatMap { now =>
          if (now < nextDue.get()) ZIO.unit
          else {
            val expired  = List.newBuilder[LockFreeHandoffCore.Waiter[A]]
            var earliest = Long.MaxValue
            val iterator = waiters.iterator()
            while (iterator.hasNext) {
              val waiter = iterator.next()
              if (!waiter.claimable) iterator.remove()
              else if (waiter.deadlineNanos <= now && waiter.claim()) {
                iterator.remove()
                waiting.decrementAndGet()
                expired += waiter
              } else if (waiter.deadlineNanos < earliest) earliest = waiter.deadlineNanos
            }
            nextDue.set(earliest)
            val victims  = expired.result()
            if (victims.isEmpty) ZIO.unit
            else
              ZIO.foreachDiscard(victims)(waiter =>
                waiter.promise.fail(new PoolTimeoutException(poolName, waiter.budget)),
              )
          }
        }
    }

  /**
   * Takes the next acquirer that can still be satisfied, dropping any that gave
   * up. A waiter is claimed as it leaves the queue, so nothing has to be
   * searched for later.
   */
  private def takeWaiter(): LockFreeHandoffCore.Waiter[A] = {
    var candidate = waiters.poll()
    while (candidate != null && !candidate.claim()) candidate = waiters.poll()
    candidate
  }

  /** Interrupted: whatever we were handed goes back, since nobody will use it. */
  private def abandon(waiter: LockFreeHandoffCore.Waiter[A]): UIO[Unit] =
    if (claimSelf(waiter)) ZIO.unit
    else waiter.promise.await.foldZIO(_ => ZIO.unit, giveBack)

  private def giveBack(acquired: Acquired[A]): UIO[Unit] =
    acquired match {
      case Acquired.Ready(resource, _) => offer(resource).unit
      case Acquired.Reserved(_)        => releaseSlot
    }

  private def claimSelf(waiter: LockFreeHandoffCore.Waiter[A]): Boolean =
    if (waiter.claim()) {
      waiting.decrementAndGet()
      true
    } else false

  /**
   * Pairs waiting acquirers with whatever is free, claiming both sides before
   * anything is promised, so nothing is handed out twice or lost.
   */
  private def pairWaiters(): List[(LockFreeHandoffCore.Waiter[A], Acquired[A])]               =
    if (waiting.get() <= 0 || closed.get()) Nil
    else {
      val paired   = List.newBuilder[(LockFreeHandoffCore.Waiter[A], Acquired[A])]
      var continue = true
      while (continue) {
        val waiter = takeWaiter()
        if (waiter == null) continue = false
        else {
          val resource = idle.pollFirst()
          if (resource != null) {
            waiting.decrementAndGet()
            paired += ((waiter, Acquired.Ready(resource, waited = true)))
          } else if (reserve()) {
            waiting.decrementAndGet()
            paired += ((waiter, Acquired.Reserved(waited = true)))
          } else {
            waiter.release()
            waiters.addFirst(waiter)
            continue = false
          }
        }
      }
      paired.result()
    }
  private def complete(paired: List[(LockFreeHandoffCore.Waiter[A], Acquired[A])]): UIO[Unit] =
    if (paired.isEmpty) ZIO.unit
    else ZIO.foreachDiscard(paired)(entry => entry._1.promise.succeed(entry._2).unit)

  private[pool] def idleSnapshot: List[A] = idle.iterator().asScala.toList
}

private[pool] object LockFreeHandoffCore {

  private val QuietPause: Duration = Duration.fromMillis(50)
  private val BusyPause: Duration  = Duration.fromMillis(2)

  /** One parked acquirer, handed its outcome exactly once. */
  final private[pool] class Waiter[A](
      val promise: Promise[SQLException, HandoffCore.Acquired[A]],
      val deadlineNanos: Long,
      val budget: Duration,
  ) {
    private val taken = new AtomicBoolean(false)

    def claim(): Boolean   = taken.compareAndSet(false, true)
    def release(): Unit    = taken.set(false)
    def claimable: Boolean = !taken.get()
  }

  def make[A](poolName: String, maxSize: Int): ZIO[Scope, Nothing, HandoffCore[A]] =
    ZIO.succeed(new LockFreeHandoffCore[A](poolName, maxSize)).tap(_.reaper.forkScoped)
}
