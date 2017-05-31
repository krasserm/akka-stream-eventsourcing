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

package com.github.krasserm.ases.log.ap

import akka.actor.ActorSystem
import akka.persistence.Journal
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
import com.github.krasserm.ases.DeliveryProtocol.Delivery

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Akka Stream API for events logs managed by an Akka Persistence journal.
  *
  * @param journalId Id of the Akka Persistence journal.
  */
class AkkaPersistenceEventLog(journalId: String)(implicit system: ActorSystem) {
  private val journal = new Journal(journalId)

  /**
    * Creates a flow representing the event log identified by `persistenceId`.
    * Input events are written to the journal and emitted as output events
    * after successful write. Before input events are requested from upstream
    * the flow emits events that have been previously written to the journal
    * (recovery phase).
    *
    * During recovery, events are emitted as [[com.github.krasserm.ases.DeliveryProtocol.Delivered Delivered]]
    * messages, recovery completion is signaled as [[com.github.krasserm.ases.DeliveryProtocol.Recovered Recovered]]
    * message, both subtypes of [[com.github.krasserm.ases.DeliveryProtocol.Delivery Delivery]]. After recovery,
    * events are again emitted as [[com.github.krasserm.ases.DeliveryProtocol.Delivered Delivered]] messages.
    *
    * It is the application's responsibility to ensure that there is only a
    * single materialized instance with given `persistenceId` writing to the
    * journal.
    *
    * @param persistenceId persistence id of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the flow.
    */
  def flow[A: ClassTag](persistenceId: String): Flow[A, Delivery[A], NotUsed] =
    journal.eventLog[A](persistenceId)

  /**
    * Creates a source that replays all events of the event log identified by
    * `persistenceId`. The source completes when the end of the event log is
    * reached (i.e. it is not a live source in contrast to the output of
    * [[flow]]).
    *
    * @param persistenceId persistence id of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def source[A: ClassTag](persistenceId: String): Source[A, NotUsed] =
    journal.eventSource[A](persistenceId)

  /**
    * Creates a sink that writes events to the event log identified by
    * `persistenceId`.
    *
    * @param persistenceId persistence id of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def sink[A: ClassTag](persistenceId: String): Sink[A, Future[Done]] =
    journal.eventSink[A](persistenceId)
}
