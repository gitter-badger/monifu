package monifu.rx

import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext
import monifu.concurrent.atomic.padded.Atomic
import scala.annotation.tailrec
import java.lang.{Iterable => JavaIterable}
import java.util.{Iterator => JavaIterator}
import monifu.rx.api.SafeObserver


trait Observable[+T] {
  def subscribe(observer: Observer[T]): Unit
  
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
              onError(ex)
          }
        }
      })
    }

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
              onError(ex)
          }
        }
      })
    }

  def take(number: Int): Observable[T] =
    Observable.create { observer =>
      subscribe(new Observer[T] {
        private[this] val remainingForRequest = Atomic(number)
        private[this] val processed = Atomic(0)
        @volatile private[this] var subscription: Subscription = null

        def onSubscription(s: Subscription): Unit = {
          subscription = s
          observer.onSubscription(new Subscription {
            def cancel(): Unit = {
              subscription.cancel()
            }
            def request(n: Int): Unit = {
              val rnr = remainingForRequest.countDownToZero(n)
              if (rnr > 0) subscription.request(rnr)
            }
          })
        }

        def onNext(elem: T): Unit = {
          val processed = this.processed.incrementAndGet()
          if (processed < number)
            observer.onNext(elem)
          else if (processed == number)
            try {
              observer.onNext(elem)
              observer.onComplete()
            } finally {
              subscription.cancel()
            }
        }

        def onError(ex: Throwable): Unit =
          observer.onError(ex)

        def onComplete(): Unit =
          observer.onComplete()
      })
    }

  def foldLeft[R](seed: R)(f: (R, T) => R): Observable[R] =
    Observable.create { observer =>
      subscribe(new SafeObserver[T](observer) {
        private[this] val result = Atomic(seed)
        def handleNext(elem: T): Unit =
          try {
            result.transform(r => f(r, elem))
          } catch {
            case NonFatal(ex) => onError(ex)
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
              onError(ex)
          }
        }
      })
    }

  def doOnCompleted(cb: => Unit)(implicit ec: ExecutionContext): Observable[T] =
    Observable.create { observer =>
      subscribe(new Observer[T] {
        def onComplete(): Unit = {
          ec.execute(new Runnable {
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
  def create[T](f: Observer[T] => Unit): Observable[T] =
    new Observable[T] {
      def subscribe(observer: Observer[T]): Unit = f(observer)
    }

  def unit[T](x: T)(implicit ec: ExecutionContext): Observable[T] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def request(n: Int): Unit =
          ec.execute(new Runnable {
            def run(): Unit = {
              observer.onNext(x)
              observer.onComplete()
            }
          })

        def cancel(): Unit = ()
      })
    }

  def empty(implicit ec: ExecutionContext): Observable[Nothing] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def cancel(): Unit = ()
        def request(n: Int): Unit =
          ec.execute(new Runnable {
            def run(): Unit =
              observer.onComplete()
          })
      })
    }

  def never: Observable[Nothing] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def request(n: Int): Unit = ()
        def cancel(): Unit = ()
      })
    }

  def error(e: Throwable)(implicit ec: ExecutionContext): Observable[Nothing] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        def cancel(): Unit = ()
        def request(n: Int): Unit =
          ec.execute(new Runnable {
            def run(): Unit =
              observer.onError(e)
          })
      })
    }

  def range(from: Int, until: Int, step: Int = 1)(implicit ec: ExecutionContext): Observable[Int] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription { self =>
        private[this] val requested = Atomic(0)
        private[this] val counter = Atomic(from)

        def request(n: Int): Unit =
          if (requested.get != -1 && requested.getAndAdd(n) == 0)
            ec.execute(new Runnable {
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

  def fromIterable[T](iterable: Iterable[T])(implicit ec: ExecutionContext): Observable[T] =
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

  def fromIterable[T](iterable: JavaIterable[T])(implicit ec: ExecutionContext): Observable[T] =
    Observable.create { observer =>
      observer.onSubscription(new Subscription {
        private[this] val capacity = Atomic(0)
        private[this] var iterator: JavaIterator[T] = null
        private[this] val lock = new AnyRef

        def request(n: Int): Unit =
          if (capacity.getAndAdd(n) == 0)
            ec.execute(new Runnable {
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
