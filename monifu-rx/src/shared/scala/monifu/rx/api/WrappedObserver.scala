package monifu.rx.api

import language.higherKinds
import monifu.rx.{Subscription, Observer}

abstract class WrappedObserver[-T] protected (observer: Observer[_]) extends Observer[T] {
  @volatile protected var subscription = null : Subscription

  def onNext(elem: T): Unit

  def onError(ex: Throwable): Unit =
    observer.onError(ex)

  def onComplete(): Unit =
    observer.onComplete()

  def onSubscription(s: Subscription) = {
    subscription = s
    observer.onSubscription(s)
  }
}
