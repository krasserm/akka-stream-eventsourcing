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
import com.github.krasserm.ases.log.ap.AkkaPersistenceEventLog
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

object EventSourcingSpec {
  import EventSourcing._

  sealed trait Request
  case object GetState extends Request             // Query
  case class Increment(delta: Int) extends Request // Command
  case class Incremented(delta: Int)               // Event
  case class Response(state: Int)

  val requestHandler: RequestHandler[Int, Incremented, Request, Response] = {
    case (_, GetState)     => (Seq(), Response)
    case (s, Increment(d)) => (Seq(Incremented(d)), Response)
  }

  val eventHandler: EventHandler[Int, Incremented] =
    (s, e) => s + e.delta
}

class EventSourcingSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with ScalaFutures with StreamSpec {
  import EventSourcing._
  import EventSourcingSpec._
  import DeliveryProtocol._

  val akkaPersistenceEventLog = new AkkaPersistenceEventLog(journalId = "akka.persistence.journal.inmem")

  def testEventLog[A](stored: Seq[A] = Seq.empty): Flow[A, Delivery[A], NotUsed] =
    Flow[A].map(Delivered(_)).prepend(Source(stored).map(Delivered(_))).prepend(Source.single(Recovered))

  def akkaPersistenceEventLog[A: ClassTag](persistenceId: String): Flow[A, Delivery[A], NotUsed] =
    akkaPersistenceEventLog.flow(persistenceId)

  "An EventSourcing stage" when {
    "joined with a test event log" must {
      val processor: Flow[Request, Response, NotUsed] =
        EventSourcing(0, requestHandler, eventHandler).join(testEventLog())

      "consume commands and produce responses" in {
        val commands = Seq(1, -4, 7).map(Increment)
        val expected = Seq(1, -3, 4).map(Response)
        Source(commands).via(processor).runWith(Sink.seq).futureValue should be(expected)
      }
      "consume queries and produce responses" in {
        val commands = Seq(1, 0, 7).map {
          case 0 => GetState
          case i => Increment(i)
        }
        val expected = Seq(1, 1, 8).map(Response)
        Source(commands).via(processor).runWith(Sink.seq).futureValue should be(expected)
      }
    }
    "joined with a non-empty test event log" must {
      val processor: Flow[Request, Response, NotUsed] =
        EventSourcing(0, requestHandler, eventHandler).join(testEventLog(Seq(Identified(Incremented(1)))))

      "first recover state and then consume commands and produce responses" in {
        val commands = Seq(-4, 7).map(Increment)
        val expected = Seq(-3, 4).map(Response)
        Source(commands).via(processor).runWith(Sink.seq).futureValue should be(expected)
      }
    }
    "joined with an Akka Persistence event log" must {
      def processor(persistenceId: String): Flow[Request, Response, NotUsed] =
        EventSourcing(0, requestHandler, eventHandler).join(akkaPersistenceEventLog(persistenceId))

      "consume commands and produce responses" in {
        val commands = Seq(1, -4, 7).map(Increment)
        val expected = Seq(1, -3, 4).map(Response)
        Source(commands).via(processor("pid-1")).runWith(Sink.seq).futureValue should be(expected)
      }

      "first recover state and then consume commands and produce responses" in {
        Source.single(Identified(Incremented(1))).runWith(akkaPersistenceEventLog.sink("pid-2")).futureValue
        val commands = Seq(-4, 7).map(Increment)
        val expected = Seq(-3, 4).map(Response)
        Source(commands).via(processor("pid-2")).runWith(Sink.seq).futureValue should be(expected)
      }
    }
  }
}
