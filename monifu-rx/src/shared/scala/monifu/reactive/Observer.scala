package monifu.reactive

import monifu.reactive.observers.SubscriberAsObserver

import scala.concurrent.{ExecutionContext, Future}
import monifu.reactive.api.Ack

/**
 * The Observer from the Rx pattern is the trio of callbacks that
 * get subscribed to an Observable for receiving events.
 *
 * The events received must follow the Rx grammar, which is:
 *      onNext *   (onComplete | onError)?
 *
 * That means an Observer can receive zero or multiple events, the stream
 * ending either in one or zero `onComplete` or `onError` (just one, not both),
 * and after onComplete or onError, a well behaved Observable implementation
 * shouldn't send any more onNext events.
 */
trait Observer[-T] {
  def onNext(elem: T): Future[Ack]

  def onError(ex: Throwable): Unit

  def onComplete(): Unit
}

object Observer {
  /**
   * Given a [[Subscriber]] as defined by the [[http://www.reactive-streams.org/ Reactive Streams]]
   * specification, it builds an [[Observer]] instance compliant with the Monifu Rx implementation.
   */
  def from[T](subscriber: Subscriber[T])(implicit ec: ExecutionContext): Observer[T] = {
    SubscriberAsObserver(subscriber)
  }
}

