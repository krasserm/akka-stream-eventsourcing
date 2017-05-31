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

/**
  * Event delivery protocol of an event log.
  *
  *  - Emit 0-n replayed events as `Delivered` message.
  *  - Emit `Recovered` to signal recovery completion.
  *  - Emit 0-n live events as `Delivered` message.
  */
object DeliveryProtocol {

  sealed trait Delivery[+A]

  /**
    * Emitted by an event log to signal recovery completion (= all events have been replayed).
    */
  case object Recovered extends Delivery[Nothing]

  /**
    * Emitted by an event log to deliver an event (replayed or live).
    */
  case class Delivered[A](a: A) extends Delivery[A]
}
