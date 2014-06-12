package monifu.reactive.observers

import monifu.reactive.api.Ack.{Cancel, Continue}
import monifu.reactive.{Observer, Subscriber, Subscription}

import scala.concurrent.ExecutionContext

final class ObserverAsSubscriber[-T] private (underlying: Observer[T], bufferSize: Int)(implicit ec: ExecutionContext)
  extends Subscriber[T] {

  require(bufferSize > 0, "bufferSize must be strictly positive")

  private[this] val buffer =
    SynchronousObserverAsSubscriber(
      SynchronousBufferedObserver.unbounded(underlying),
      requestSize = bufferSize
    )

  def onSubscribe(s: Subscription): Unit =
    buffer.onSubscribe(s)

  def onNext(elem: T): Unit =
    buffer.onNext(elem)

  def onError(ex: Throwable): Unit =
    buffer.onError(ex)

  def onComplete(): Unit =
    buffer.onComplete()
}


object ObserverAsSubscriber {
  def apply[T](observer: Observer[T], bufferSize: Int = 128)(implicit ec: ExecutionContext): ObserverAsSubscriber[T] = {
    new ObserverAsSubscriber[T](observer, bufferSize)
  }
}

final class SynchronousObserverAsSubscriber[-T] private (underlying: SynchronousObserver[T], requestSize: Int)(implicit ec: ExecutionContext)
  extends Subscriber[T] {

  require(requestSize != 0, "requestSize cannot be zero")

  private[this] var subscription = null : Subscription
  private[this] var expectingCount = 0
  @volatile private[this] var isCanceled = false

  def onSubscribe(s: Subscription): Unit =
    if (!isCanceled) {
      subscription = s
      expectingCount = requestSize
      s.request(requestSize)
    }

  def onNext(elem: T): Unit = {
    if (!isCanceled) {
      if (expectingCount > 0) expectingCount -= 1

      underlying.onNext(elem) match {
        case Continue =>
          // should it request more events?
          if (expectingCount == 0) {
            expectingCount = requestSize
            subscription.request(requestSize)
          }
        case Cancel =>
          // downstream canceled, so we MUST cancel too
          isCanceled = true
          subscription.cancel()
      }
    }
  }

  def onError(ex: Throwable): Unit = {
    if (!isCanceled) {
      isCanceled = true
      underlying.onError(ex)
    }
  }

  def onComplete(): Unit = {
    if (!isCanceled) {
      isCanceled = true
      underlying.onComplete()
    }
  }
}


object SynchronousObserverAsSubscriber {
  def apply[T](observer: SynchronousObserver[T], requestSize: Int = -1)(implicit ec: ExecutionContext) =
    new SynchronousObserverAsSubscriber[T](observer, requestSize)
}