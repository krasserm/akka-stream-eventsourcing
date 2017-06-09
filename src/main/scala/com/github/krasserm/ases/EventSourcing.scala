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

import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import com.github.krasserm.ases.DeliveryProtocol._
import com.github.krasserm.ases.EventSourcing._

import scala.collection.immutable.Seq

object EventSourcing {
  /**
    * Event handler. Input is the current state and an event that has been written to an
    * event log. Output is the updated state that is set to the current state by the
    * [[EventSourcing]] driver.
    *
    * @tparam S State type.
    * @tparam E Event type.
    */
  type EventHandler[S, E] =
    (S, E) => S

  /**
    * Request handler. Input is the current state and the current request. Output is either
    * a result created with [[respond]] or with [[emit]].
    *
    *  - [[respond]] creates an immediate response which can be either the response to a ''query''
    *  or the failure response to a ''command'' whose validation failed, for example.
    *  - [[emit]] returns a sequence of events to be written to an event log and a response factory
    *  to be called with the current state after all written events have been applied to it.
    *
    * @tparam S State type.
    * @tparam E Event type.
    * @tparam REQ Request type.
    * @tparam RES Response type.
    */
  type RequestHandler[S, E, REQ, RES] =
    (S, REQ) => RequestHandlerResult[S, E, RES]

  sealed trait RequestHandlerResult[S, E, RES]

  private case class Respond[S, E, RES](response: RES)
    extends RequestHandlerResult[S, E, RES]

  private case class Emit[S, E, RES](events: Seq[E], responseFactory: S => RES)
    extends RequestHandlerResult[S, E, RES] {
    require(events.nonEmpty, "event sequence must not be empty")
  }

  /**
    * Creates a request handler result that contains an immediate response.
    */
  def respond[S, E, RES](response: RES): RequestHandlerResult[S, E, RES] =
    Respond(response)

  /**
    * Create a request handler result that contains events to be written to an event log and a response
    * factory to be called with the current state after all written events have been applied to it.
    */
  def emit[S, E, RES](events: Seq[E], responseFactory: S => RES): RequestHandlerResult[S, E, RES] =
    Emit(events, responseFactory)

  /**
    * Used by [[EventSourcing]] to correlate emitted events with input events (preliminary solution).
    */
  case class Identified[A](id: String, event: A)

  object Identified {
    def apply[A](event: A): Identified[A] =
      Identified(UUID.randomUUID().toString, event)
  }

  /**
    * Creates a bidi-flow that implements the driver for event sourcing logic defined by `requestHandler`
    * `eventHandler`. The created event sourcing stage should be joined with an event log (i.e. a flow)
    * for writing emitted events. Written events are delivered from the joined event log back to the stage:
    *
    *  - After materialization, the stage's state is recovered with replayed events delivered by the joined
    *    event log.
    *  - On recovery completion (see [[DeliveryProtocol]]) the stage is ready to accept requests if there is
    *    downstream response and event demand.
    *  - On receiving a command it calls the request handler and emits the returned events. The emitted events
    *    are sent downstream to the joined event log.
    *  - For each written event that is delivered from the event log back to the event sourcing stage, the
    *    event handler is called with that event and the current state. The stage updates its current state
    *    with the event handler result.
    *  - After all emitted events (for a given command) have been applied, the response function, previously
    *    created by the command handler, is called with the current state and the created response is emitted.
    *  - After response emission, the stage is ready to accept the next request if there is downstream response
    *    and event demand.
    *
    * @param initial Initial state.
    * @param requestHandler The stage's request handler.
    * @param eventHandler The stage's event handler.
    * @tparam S State type.
    * @tparam E Event type.
    * @tparam REQ Request type.
    * @tparam RES Response type.
    */
  def apply[S, E, REQ, RES](
      initial: S,
      requestHandler: RequestHandler[S, E, REQ, RES],
      eventHandler: EventHandler[S, E]): BidiFlow[REQ, Identified[E], Delivery[Identified[E]], RES, NotUsed] =
    BidiFlow.fromGraph(new EventSourcing[S, E, REQ, RES](initial, _ => requestHandler, _ => eventHandler))

  /**
    * Creates a bidi-flow that implements the driver for event sourcing logic returned by `requestHandlerProvider`
    * and `eventHandlerProvider`. `requestHandlerProvider` is evaluated with current state for each received request,
    * `eventHandlerProvider` is evaluated with current state for each written event. This can be used by applications
    * to switch request and event handling logic as a function of current state. The created event sourcing stage
    * should be joined with an event log (i.e. a flow) for writing emitted events. Written events are delivered
    * from the joined event log back to the stage:
    *
    *  - After materialization, the stage's state is recovered with replayed events delivered by the joined
    *    event log.
    *  - On recovery completion (see [[DeliveryProtocol]]) the stage is ready to accept requests if there is
    *    downstream response and event demand.
    *  - On receiving a command it calls the request handler and emits the returned events. The emitted events
    *    are sent downstream to the joined event log.
    *  - For each written event that is delivered from the event log back to the event sourcing stage, the
    *    event handler is called with that event and the current state. The stage updates its current state
    *    with the event handler result.
    *  - After all emitted events (for a given command) have been applied, the response function, previously
    *    created by the command handler, is called with the current state and the created response is emitted.
    *  - After response emission, the stage is ready to accept the next request if there is downstream response
    *    and event demand.
    *
    * @param initial Initial state.
    * @param requestHandlerProvider The stage's request handler provider.
    * @param eventHandlerProvider The stage's event handler provider.
    * @tparam S State type.
    * @tparam E Event type.
    * @tparam REQ Request type.
    * @tparam RES Response type.
    */
  def apply[S, E, REQ, RES](
      initial: S,
      requestHandlerProvider: S => RequestHandler[S, E, REQ, RES],
      eventHandlerProvider: S => EventHandler[S, E]): BidiFlow[REQ, Identified[E], Delivery[Identified[E]], RES, NotUsed] =
    BidiFlow.fromGraph(new EventSourcing(initial, requestHandlerProvider, eventHandlerProvider))
}

private class EventSourcing[S, E, REQ, RES](
    initial: S,
    requestHandlerProvider: S => RequestHandler[S, E, REQ, RES],
    eventHandlerProvider: S => EventHandler[S, E])
  extends GraphStage[BidiShape[REQ, Identified[E], Delivery[Identified[E]], RES]] {

  private case class Roundtrip(eventIds: Set[String], responseFactory: S => RES) {
    def delivered(eventId: String): Roundtrip = copy(eventIds - eventId)
  }

  val ci = Inlet[REQ]("EventSourcing.requestIn")
  val eo = Outlet[Identified[E]]("EventSourcing.eventOut")
  val ei = Inlet[Delivery[Identified[E]]]("EventSourcing.eventIn")
  val ro = Outlet[RES]("EventSourcing.responseOut")

  val shape = BidiShape.of(ci, eo, ei, ro)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var requestUpstreamFinished = false
      private var recovered = false
      private var roundtrip: Option[Roundtrip] = None
      private var state: S = initial

      setHandler(ci, new InHandler {
        override def onPush(): Unit = {
          requestHandlerProvider(state)(state, grab(ci)) match {
            case Respond(response) =>
              push(ro, response)
              tryPullCi()
            case Emit(events, responseFactory) =>
              val identified = events.map(Identified(_))
              roundtrip = Some(Roundtrip(identified.map(_.id)(collection.breakOut), responseFactory))
              emitMultiple(eo, identified)
          }
        }

        override def onUpstreamFinish(): Unit =
          requestUpstreamFinished = true
      })

      setHandler(ei, new InHandler {
        override def onPush(): Unit = {
          grab(ei) match {
            case Recovered =>
              recovered = true
              tryPullCi()
            case Delivered(identified) =>
              state = eventHandlerProvider(state)(state, identified.event)
              roundtrip = roundtrip.map(_.delivered(identified.id)).flatMap {
                case r if r.eventIds.isEmpty =>
                  push(ro, r.responseFactory(state))
                  if (requestUpstreamFinished) completeStage() else tryPullCi()
                  None
                case r =>
                  Some(r)
              }
          }
          tryPullEi()
        }
      })

      setHandler(eo, new OutHandler {
        override def onPull(): Unit =
          tryPullCi()
      })

      setHandler(ro, new OutHandler {
        override def onPull(): Unit =
          tryPullCi()
      })

      override def preStart(): Unit =
        tryPullEi()

      private def tryPullEi(): Unit =
        if (!requestUpstreamFinished) pull(ei)

      private def tryPullCi(): Unit =
        if (isAvailable(eo) && isAvailable(ro) && !hasBeenPulled(ci) && roundtrip.isEmpty && !requestUpstreamFinished) pull(ci)
    }
}