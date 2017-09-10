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
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit.TestKit
import com.github.krasserm.ases.log.AkkaPersistenceEventLog
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}
import scala.collection.immutable.Seq

object RequestRoutingSpec {
  import EventSourcing._

  sealed trait Request {
    def aggregateId: String
  }
  case class GetState(aggregateId: String) extends Request              // Query
  case class Increment(aggregateId: String, delta: Int) extends Request // Command
  case class Incremented(aggregateId: String, delta: Int)               // Event
  case class Response(aggregateId: String, state: Int)

  val requestHandler: RequestHandler[Int, Incremented, Request, Response] = {
    case (s, GetState(aggregateId))     => respond(Response(aggregateId, s))
    case (_, Increment(aggregateId, d)) => emit(Seq(Incremented(aggregateId, d)), Response(aggregateId, _))
  }

  val eventHandler: EventHandler[Int, Incremented] =
    (s, e) => s + e.delta
}

class RequestRoutingSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with ScalaFutures with StreamSpec {
  import RequestRoutingSpec._

  val akkaPersistenceEventLog: AkkaPersistenceEventLog =
    new log.AkkaPersistenceEventLog(journalId = "akka.persistence.journal.inmem")

  def processor(aggregateId: String): Flow[Request, Response, NotUsed] =
    EventSourcing(aggregateId, 0, requestHandler, eventHandler).join(akkaPersistenceEventLog.flow(aggregateId))

  def router: Flow[Request, Response, NotUsed] =
    Router(_.aggregateId, processor)

  "A request router" when {
    "configured to route based on aggregate id" must {
      "dynamically create a request processor for each aggregate id" in {
        val aggregateId1 = "a1"
        val aggregateId2 = "a2"

        val (pub, sub) = probes(router)

        pub.sendNext(Increment(aggregateId1, 3))
        sub.requestNext(Response(aggregateId1, 3))

        pub.sendNext(Increment(aggregateId2, 1))
        sub.requestNext(Response(aggregateId2, 1))

        pub.sendNext(Increment(aggregateId1, 2))
        sub.requestNext(Response(aggregateId1, 5))

        pub.sendNext(Increment(aggregateId2, -4))
        sub.requestNext(Response(aggregateId2, -3))
      }
      "handle single command using Source.single" in {
        val request = Increment("a3", 3)
        val expected = Response("a3", 3)
        Source.single(request)
          .via(router)
          .runWith(Sink.head)
          .futureValue should be(expected)
      }
      "handle single command using Source.apply(Seq)" in {
        val request = Increment("a4", 3)
        val expected = Response("a4", 3)
        Source(Seq(request))
          .via(router)
          .runWith(Sink.head)
          .futureValue should be(expected)
      }
      "handle multiple commands" in {
        Source(Seq(Increment("a5", 1), Increment("a5", 2), Increment("a5", 3)))
          .via(router)
          .runWith(Sink.seq)
          .futureValue should be(Seq(Response("a5", 1), Response("a5", 3), Response("a5", 6)))
      }
    }
  }
}