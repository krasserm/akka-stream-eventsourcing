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

package com.github.krasserm.ases.log

import scala.concurrent.Future
import scala.reflect.ClassTag
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.github.krasserm.ases.{Delivery, Durable, Emitted}

/**
  * Akka Stream API for events logs. Each individual event log is identified
  * by an identifier. The type of the identifier (supplied as type argument
  * `Identifier`) will depend on the concrete event log implementation.
  */
trait EventLog[Identifier] {

  /**
    * Creates a flow representing the event log identified by `identifier`.
    * Input events are written to the journal and emitted as output events
    * after successful write. Before input events are requested from upstream
    * the flow emits events that have been previously written to the journal
    * (recovery phase).
    *
    * During recovery, events are emitted as [[Delivered]] messages, recovery completion
    * is signaled as [[Recovered]] message, both subtypes of [[Delivery]]. After recovery,
    * events are again emitted as [[Delivered]] messages.
    *
    * It is the application's responsibility to ensure that there is only a
    * single materialized instance with given `persistenceId` writing to the
    * journal.
    *
    * @param identifier identifier of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the flow.
    */
  def flow[A: ClassTag](identifier: Identifier): Flow[Emitted[A], Delivery[Durable[A]], NotUsed]

  /**
    * Creates a source that replays events of the event log identified by `identifier`.
    * The source completes when the end of the log has been reached.
    *
    * During recovery, events are emitted as [[Delivered]] messages, recovery completion
    * is signaled as [[Recovered]] message, both subtypes of [[Delivery]].
    *
    * @param identifier identifier of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def source[A: ClassTag](identifier: Identifier): Source[Delivery[Durable[A]], NotUsed]

  /**
    * Creates a sink that writes events to the event log identified by
    * `identifier`.
    *
    * @param identifier persistence id of the event log.
    */
  def sink[A](identifier: Identifier): Sink[Emitted[A], Future[Done]]

}
