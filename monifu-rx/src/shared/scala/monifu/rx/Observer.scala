package monifu.rx

trait Observer[-T] {
  def onSubscription(s: Subscription): Unit
  def onNext(elem: T): Unit
  def onError(ex: Throwable): Unit
  def onComplete(): Unit
}
