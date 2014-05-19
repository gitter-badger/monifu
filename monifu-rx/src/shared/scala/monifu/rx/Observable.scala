package monifu.rx

import scala.util.control.NonFatal
import monifu.concurrent.atomic.padded.Atomic
import scala.annotation.tailrec
import java.lang.{Iterable => JavaIterable}
import java.util.{Iterator => JavaIterator}
import monifu.rx.api.SafeObserver
import monifu.concurrent.Scheduler


/**
 * Representation for the Observable in the Rx pattern.
 */
trait Observable[+T] {
  /**
   * Function that creates the actual subscription when calling `subscribe`,
   * and that starts the stream, being meant to be overridden in custom combinators
   * or in classes implementing Observable.
   *
   * @param observer is an [[Observer]] on which `onNext`, `onComplete` and `onError`
   *                 happens, according to the Rx grammar.
   *
   * @return a cancelable that can be used to cancel the streaming
   */
  def subscribe(observer: Observer[T]): Unit

  /**
   * Implicit `scala.concurrent.ExecutionContext` under which our computations will run.
   */
  protected implicit def scheduler: Scheduler

  /**
   * Returns an Observable that applies the given function to each item emitted by an
   * Observable and emits the result.
   *
   * @param f a function to apply to each item emitted by the Observable
   * @return an Observable that emits the items from the source Observable, transformed by the given function
   */
  def map[U](f: T => U): Observable[U] =
    Observable.create { observer =>
      subscribe(new SafeObserver[T](observer) {
        def handleNext(elem: T): Unit = {
          var streamError = true
          try {
            val r = f(elem)
            streamError = false
            observer.onNext(r)
          }
          catch {
            case NonFatal(ex) if streamError =>
              try subscription.cancel() finally onError(ex)
          }
        }
      })
    }

  /**
   * Returns an Observable which only emits those items for which the given predicate holds.
   *
   * @param p a function that evaluates the items emitted by the source Observable, returning `true` if they pass the filter
   * @return an Observable that emits only those items in the original Observable for which the filter evaluates as `true`
   */
  def filter(p: T => Boolean): Observable[T] =
    Observable.create { observer =>
      subscribe(new SafeObserver[T](observer) {
        def handleNext(elem: T): Unit = {
          var streamError = true
          try {
            val isValid = p(elem)
            streamError = false
            if (isValid) observer.onNext(elem)
          }
          catch {
            case NonFatal(ex) if streamError =>
              try subscription.cancel() finally onError(ex)
          }
        }
      })
    }

  /**
   * Selects the first ''n'' elements (from the start).
   *
   *  @param n the number of elements to take
   *  @return a new Observable that emits only the first ''n'' elements from the source
   */
  def take(n: Int): Observable[T] =
    Observable.create { observer =>
      subscribe(new SafeObserver[T](observer) {
        private[this] val remainingForRequest = Atomic(n)
        private[this] val processed = Atomic(0)

        override def handleSubscription(s: Subscription) =
          new Subscription {
            def cancel(): Unit = {
              s.cancel()
            }
            def request(n: Int): Unit = {
              val rnr = remainingForRequest.countDownToZero(n)
              if (rnr > 0) s.request(rnr)
            }
          }

        def handleNext(elem: T): Unit = {
          val processed = this.processed.incrementAndGet()
          if (processed < n)
            observer.onNext(elem)
          else if (processed == n)
            try {
              observer.onNext(elem)
              observer.onComplete()
            } finally {
              subscription.cancel()
            }
        }
      })
    }

  /**
   * Applies a binary operator to a start value and all elements of this Observable,
   * going left to right and returns a new Observable that emits only one item
   * before `onCompleted`.
   */
  def foldLeft[R](seed: R)(f: (R, T) => R): Observable[R] =
    Observable.create { observer =>
      subscribe(new SafeObserver[T](observer) {
        private[this] val result = Atomic(seed)

        def handleNext(elem: T): Unit =
          try {
            result.transform(r => f(r, elem))
          } catch {
            case NonFatal(ex) =>
              try subscription.cancel() finally onError(ex)
          }

        override def handleComplete(): Unit = {
          observer.onNext(result.get)
          observer.onComplete()
        }
      })
    }

  def scan[R](seed: R)(f: (R, T) => R): Observable[R] =
    Observable.create { observer =>
      subscribe(new SafeObserver[T](observer) {
        private[this] val result = Atomic(seed)

        def handleNext(elem: T): Unit = {
          var streamError = true
          try {
            val next = result.transformAndGet(r => f(r, elem))
            streamError = false
            observer.onNext(next)
          } catch {
            case NonFatal(ex) if streamError =>
              try subscription.cancel() finally onError(ex)
          }
        }
      })
    }

  /**
   * Executes the given callback asynchronously, when the stream has
   * ended on `onCompleted`.
   *
   * NOTE: protect the callback such that it doesn't throw exceptions, because
   * it gets executed when `cancel()` happens and by definition the error cannot
   * be streamed with `onError()` and so the behavior is left as undefined, possibly
   * crashing the application or worse - leading to non-deterministic behavior.
   *
   * @param cb the callback to execute when the subscription is canceled
   */
  def doOnCompleted(cb: => Unit): Observable[T] =
    Observable.create { observer =>
      subscribe(new Observer[T] {
        def onComplete(): Unit = {
          scheduler.execute(new Runnable {
            def run(): Unit = cb
          })
          observer.onComplete()
        }

        def onError(ex: Throwable): Unit =
          observer.onError(ex)
        def onNext(elem: T): Unit =
          observer.onNext(elem)
        def onSubscription(s: Subscription): Unit =
          observer.onSubscription(s)
      })
    }

  def foreach(cb: T => Unit): Unit =
    subscribe(new Observer[T] {
      @volatile
      private[this] var sub: Subscription = null
      private[this] val requested = Atomic(0)

      @tailrec
      private[this] def requestMore(): Unit = {
        val current = requested.get
        val update = if (current == 0) Int.MaxValue else current - 1
        if (!requested.compareAndSet(current, update))
          requestMore()
        else if (update == Int.MaxValue)
          sub.request(Int.MaxValue)
      }

      def onComplete() = ()
      def onError(ex: Throwable): Unit = throw ex
      def onSubscription(s: Subscription): Unit = {
        sub = s
        requestMore()
      }

      def onNext(elem: T): Unit = {
        var streamError = true
        try {
          cb(elem)
          streamError = false
          requestMore()
        }
        catch {
          case NonFatal(ex) if streamError =>
            sub.cancel()
            onError(ex)
        }
      }
    })
}

object Observable {
  def create[T](f: Observer[T] => Unit)(implicit scheduler: Scheduler): Observable[T] = {
    val s = scheduler
    new Observable[T] {
      def subscribe(observer: Observer[T]): Unit = f(observer)
      val scheduler = s
    }
  }

  def unit[T](x: T)(implicit scheduler: Scheduler): Observable[T] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def request(n: Int): Unit =
          scheduler.execute(new Runnable {
            def run(): Unit = {
              observer.onNext(x)
              observer.onComplete()
            }
          })

        def cancel(): Unit = ()
      })
    }

  def empty(implicit scheduler: Scheduler): Observable[Nothing] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def cancel(): Unit = ()
        def request(n: Int): Unit =
          scheduler.execute(new Runnable {
            def run(): Unit =
              observer.onComplete()
          })
      })
    }

  def never(implicit scheduler: Scheduler): Observable[Nothing] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def request(n: Int): Unit = ()
        def cancel(): Unit = ()
      })
    }

  def error(e: Throwable)(implicit scheduler: Scheduler): Observable[Nothing] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def cancel(): Unit = ()
        def request(n: Int): Unit =
          scheduler.execute(new Runnable {
            def run(): Unit =
              observer.onError(e)
          })
      })
    }

  def range(from: Int, until: Int, step: Int = 1)(implicit scheduler: Scheduler): Observable[Int] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription { self =>
        private[this] val requested = Atomic(0)
        private[this] val counter = Atomic(from)

        def request(n: Int): Unit =
          if (requested.get != -1 && requested.getAndAdd(n) == 0)
            scheduler.execute(new Runnable {
              def run(): Unit = {
                while (true) {
                  val requested = self.requested.get
                  var loops = requested
                  var isCompleted = false
                  var counter = self.counter.get

                  while (loops > 0)
                    if ((until > from && counter < until) || (until < from && counter > until)) {
                      observer.onNext(counter)
                      counter += step
                      loops -= 1
                    } else {
                      observer.onComplete()
                      loops = 0
                      isCompleted = true
                    }

                  self.counter.set(counter)
                  if (isCompleted) {
                    self.requested.set(0)
                    return
                  }
                  else if (self.requested.decrementAndGet(requested) == 0)
                    return
                }
              }
            })

        def cancel(): Unit =
          requested.set(-1)
      })
    }

  def fromIterable[T](iterable: Iterable[T])(implicit scheduler: Scheduler): Observable[T] =
    fromIterable(new JavaIterable[T] {
      def iterator(): JavaIterator[T] = {
        new JavaIterator[T] {
          private[this] val i = iterable.iterator
          def next(): T = i.next()
          def hasNext: Boolean = i.hasNext
          def remove(): Unit = throw new UnsupportedOperationException()
        }
      }
    })

  def fromIterable[T](iterable: JavaIterable[T])(implicit scheduler: Scheduler): Observable[T] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        private[this] val capacity = Atomic(0)
        private[this] var iterator: JavaIterator[T] = null
        private[this] val lock = new AnyRef

        def request(n: Int): Unit =
          if (capacity.getAndAdd(n) == 0)
            scheduler.execute(new Runnable {
              def run(): Unit =
                lock.synchronized {
                  if (iterator == null) iterator = iterable.iterator()
                  while (capacity.get > 0) {
                    var streamError = true
                    try {
                      if (!iterator.hasNext) {
                        streamError = false
                        observer.onComplete()
                        capacity.set(0)
                        return
                      }
                      else {
                        val elem = iterator.next()
                        streamError = false
                        observer.onNext(elem)
                        capacity.decrement()
                      }
                    }
                  }
                }
            })

        def cancel(): Unit = {
          capacity.set(0)
        }
      })
    }
}
