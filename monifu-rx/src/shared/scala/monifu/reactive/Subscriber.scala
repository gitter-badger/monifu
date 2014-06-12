package monifu.reactive

import monifu.reactive.observers.{ObserverAsSubscriber, SynchronousObserver, SynchronousObserverAsSubscriber}
import scala.concurrent.ExecutionContext

/**
 * Mirrors the `Subscriber` interface from the
 * [[http://www.reactive-streams.org/ Reactive Streams]] project.
 */
trait Subscriber[-T] {
  def onSubscribe(s: Subscription): Unit
  def onNext(elem: T): Unit
  def onError(ex: Throwable): Unit
  def onComplete(): Unit
}

object Subscriber {
  /**
   * Given an [[Observer]], builds a [[Subscriber]] instance as defined by the
   * [[http://www.reactive-streams.org/ Reactive Streams]] specification.
   */
  def from[T](observer: Observer[T], requestSize: Int = 128)(implicit ec: ExecutionContext): Subscriber[T] = {
    observer match {
      case sync: SynchronousObserver[_] =>
        val inst = sync.asInstanceOf[SynchronousObserver[T]]
        SynchronousObserverAsSubscriber(inst, requestSize)
      case async =>
        ObserverAsSubscriber(async, requestSize)
    }
  }
}

