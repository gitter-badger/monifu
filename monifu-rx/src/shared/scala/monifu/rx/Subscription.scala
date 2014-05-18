package monifu.rx

import monifu.concurrent.Cancelable

trait Subscription extends Cancelable {
  def request(n: Int): Unit
  def cancel(): Unit
}
