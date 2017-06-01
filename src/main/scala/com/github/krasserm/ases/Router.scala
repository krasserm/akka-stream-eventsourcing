/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.krasserm.ases

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}

object Router {
  /**
    * Creates a router that routes to different `processors` based on input element key `K`.
    * A key is computed from input elements with the `key` function. Whenever a new key is
    * encountered, a new key-specific processor is created with the `processor` function.
    * A processor processes all input elements of given key. Processor output elements
    * are merged back into the main flow returned by this method.
    *
    * @param key computes a key from an input element
    * @param processor key-specific processor factory
    * @param maxProcessors maximum numbers of concurrent processors.
    * @tparam A router and processor input type
    * @tparam B router and processor output type
    * @tparam K key type
    */
  def apply[A, B, K](key: A => K, processor: K => Flow[A, B, NotUsed], maxProcessors: Int = Int.MaxValue): Flow[A, B, NotUsed] =
    Flow[A].groupBy(maxProcessors, key).prefixAndTail(1).flatMapMerge(maxProcessors, {
      case (h, t) => Source(h).concat(t).via(processor(key(h.head)))
    }).mergeSubstreams
}
