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
 
package monifu.reactive

/**
 * A `Subject` is a sort of bridge or proxy that acts both as an
 * [[Observer]] and as an [[Observable]] and that must respect the contract of both.
 *
 * Because it is a `Observer`, it can subscribe to an `Observable` and because it is an `Observable`,
 * it can pass through the items it observes by re-emitting them and it can also emit new items.
 *
 * Useful to build multicast Observables or reusable processing pipelines.
 */
trait Subject[-I, +T] extends Observable[T] with Observer[I]

