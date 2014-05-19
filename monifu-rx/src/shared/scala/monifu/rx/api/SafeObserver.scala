package monifu.rx.api

import language.higherKinds
import monifu.rx.{Subscription, Observer}
import monifu.concurrent.atomic.padded.Atomic

abstract class SafeObserver[-T] protected (observer: Observer[_]) extends Observer[T] {
  private[this] val isComplete = Atomic(false)
  @volatile private[this] var _subscription: Subscription = null
  protected def subscription = _subscription

  def handleNext(elem: T): Unit

  def handleError(ex: Throwable): Unit = {
    try observer.onError(ex) finally _subscription.cancel()
  }

  def handleComplete(): Unit = {
    observer.onComplete()
  }

  def handleSubscription(s: Subscription): Subscription = s

  final def onNext(elem: T): Unit =
    if (!isComplete.get) handleNext(elem)

  final def onError(ex: Throwable): Unit =
    if (isComplete.compareAndSet(expect=false, update=true)) {
      handleError(ex)
    }

  final def onComplete(): Unit =
    if (isComplete.compareAndSet(expect=false, update=true)) {
      handleComplete()
    }

  final def onSubscription(s: Subscription) = {
    _subscription = handleSubscription(s)
    observer.onSubscription(new Subscription {
      def cancel(): Unit = {
        isComplete set true
        subscription.cancel()
      }

      def request(n: Int): Unit =
        s.request(n)
    })
  }
}

object SafeObserver {
  def apply[T](observer: Observer[T]): SafeObserver[T] =
    observer match {
      case ref: SafeObserver[_] => ref.asInstanceOf[SafeObserver[T]]
      case _ => new SafeObserver[T](observer) {
        def handleNext(elem: T): Unit = observer.onNext(elem)
      }
    }
}
