package monifu.reactive

/**
 * Mirrors the `Subscriber` interface from the
 * [[http://www.reactive-streams.org/ Reactive Streams]] project.
 */
trait Publisher[+T] {
  def subscribe(subscriber: Subscriber[T]): Unit
}
