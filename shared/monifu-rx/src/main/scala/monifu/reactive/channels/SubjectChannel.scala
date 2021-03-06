/*
 * Copyright (c) 2014 by its authors. Some rights reserved.
 * See the project homepage at
 *
 *     http://www.monifu.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package monifu.reactive.channels

import monifu.concurrent.Scheduler
import monifu.reactive.BufferPolicy.Unbounded
import monifu.reactive.observers.BufferedObserver
import monifu.reactive.{BufferPolicy, Channel, Observable, Observer, Subject}

/**
 * Wraps any [[Subject]] into a [[Channel]].
 */
class SubjectChannel[-I,+O](subject: Subject[I, O], policy: BufferPolicy)
    (implicit scheduler: Scheduler)
  extends Channel[I] with Observable[O] {

  private[this] val channel = BufferedObserver(subject, policy)

  final def subscribeFn(observer: Observer[O]): Unit = {
    subject.unsafeSubscribe(observer)
  }

  final def pushNext(elems: I*): Unit = {
    for (elem <- elems) channel.onNext(elem)
  }

  final def pushComplete(): Unit = {
    channel.onComplete()
  }

  final def pushError(ex: Throwable): Unit = {
    channel.onError(ex)
  }
}

object SubjectChannel {
  /**
   * Wraps any [[Subject]] into a [[Channel]].
   */
  def apply[I,O](subject: Subject[I, O], bufferPolicy: BufferPolicy = Unbounded)
      (implicit s: Scheduler): SubjectChannel[I, O] = {
    new SubjectChannel[I,O](subject, bufferPolicy)
  }
}
