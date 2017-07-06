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

import akka.actor.ActorSystem
import akka.persistence.{Journal, PersistentRepr}
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import com.github.krasserm.ases._

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Akka Stream API for events logs managed by an Akka Persistence journal.
  * An individual event log is identified by `persistenceId`.
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
    * During recovery, events are emitted as [[Delivered]] messages, recovery completion
    * is signaled as [[Recovered]] message, both subtypes of [[Delivery]]. After recovery,
    * events are again emitted as [[Delivered]] messages.
    *
    * It is the application's responsibility to ensure that there is only a
    * single materialized instance with given `persistenceId` writing to the
    * journal.
    *
    * @param persistenceId persistence id of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the flow.
    */
  def flow[A: ClassTag](persistenceId: String, fromSequenceNr: Long = 1L): Flow[Emitted[A], Delivery[Durable[A]], NotUsed] =
    AkkaPersistenceCodec[A](persistenceId).join(journal.eventLog(persistenceId, fromSequenceNr))

  /**
    * Creates a source that replays events of the event log identified by `persistenceId`.
    * The source completes when the end of the log has been reached.
    *
    * During recovery, events are emitted as [[Delivered]] messages, recovery completion
    * is signaled as [[Recovered]] message, both subtypes of [[Delivery]].
    *
    * @param persistenceId persistence id of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def source[A: ClassTag](persistenceId: String, fromSequenceNr: Long = 1L): Source[Delivery[Durable[A]], NotUsed] =
    journal.eventSource(persistenceId, fromSequenceNr).via(AkkaPersistenceCodec.decoder)

  /**
    * Creates a sink that writes events to the event log identified by
    * `persistenceId`.
    *
    * @param persistenceId persistence id of the event log.
    */
  def sink[A](persistenceId: String): Sink[Emitted[A], Future[Done]] =
    AkkaPersistenceCodec.encoder(persistenceId).toMat(journal.eventSink(persistenceId))(Keep.right)
}

/**
  * Codec for translating [[Emitted]] to [[PersistentRepr]] and [[PersistentRepr]] to [[Durable]].
  */
private object AkkaPersistenceCodec {
  /**
    * Codec composed of [[encoder]] and [[decoder]].
    */
  def apply[A: ClassTag](persistenceId: String): BidiFlow[Emitted[A], PersistentRepr, Delivery[PersistentRepr], Delivery[Durable[A]], NotUsed] =
    BidiFlow.fromFlows(encoder(persistenceId), decoder)

  /**
    * Decodes a [[PersistentRepr]] event as [[Durable]]
    */
  def decoder[A: ClassTag]: Flow[Delivery[PersistentRepr], Delivery[Durable[A]], NotUsed] =
    Flow[Delivery[PersistentRepr]].collect {
      case Delivered(PersistentRepr(Emitted(e: A, emitterId, emissionUuid), sequenceNr)) =>
        Delivered(Durable(e, emitterId, emissionUuid, sequenceNr))
      case Recovered =>
        Recovered
    }

  /**
    * Encodes an [[Emitted]] event as [[PersistentRepr]].
    */
  def encoder[A](persistenceId: String): Flow[Emitted[A], PersistentRepr, NotUsed] =
    Flow[Emitted[A]].map(encode(persistenceId))

  private def encode[A](persistenceId: String)(emitted: Emitted[A]): PersistentRepr =
    PersistentRepr(
      payload = emitted,
      sequenceNr = -1L,
      persistenceId = persistenceId,
      writerUuid = PersistentRepr.Undefined,
      manifest = PersistentRepr.Undefined,
      deleted = false,
      sender = null)
}