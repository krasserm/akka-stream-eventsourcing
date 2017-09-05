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
import com.github.krasserm.ases.log.{AkkaPersistenceEventLog, KafkaEventLog, KafkaSpec}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.immutable.Seq

object EventSourcingSpec {
  import EventSourcing._

  sealed trait Request
  sealed trait Event {
    def delta: Int
  }
  case object GetState extends Request                      // Query
  case class Increment(delta: Int) extends Request          // Command
  case class ClearIfEqualTo(value: Int) extends Request     // Command
  case class Incremented(delta: Int) extends Event          // Event
  case class Cleared(value: Int) extends Event {            // Event
    override def delta = -value
  }
  case class Response(state: Int)

  val requestHandler: RequestHandler[Int, Event, Request, Response] = {
    case (s, req @ GetState)      =>
      respond(Response(s))
    case (s, req @ Increment(d))  =>
      emit(Seq(Incremented(d)), Response)
    case (s, req @ ClearIfEqualTo(v))    =>
      if (s == v) emit(Seq(Cleared(v)), Response)
      else respond(Response(s))
  }

  val eventHandler: EventHandler[Int, Event] =
    (s, e) => s + e.delta
}

class EventSourcingSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with ScalaFutures with StreamSpec with KafkaSpec {
  import EventSourcingSpec._

  implicit val pc = PatienceConfig(timeout = Span(5, Seconds), interval = Span(10, Millis))

  val akkaPersistenceEventLog: AkkaPersistenceEventLog =
    new log.AkkaPersistenceEventLog(journalId = "akka.persistence.journal.inmem")

  val kafkaEventLog: KafkaEventLog =
    new log.KafkaEventLog(host, port)

  def testEventLog[A](emitted: Seq[Emitted[A]] = Seq.empty): Flow[Emitted[A], Delivery[Durable[A]], NotUsed] =
    Flow[Emitted[A]]
      .zipWithIndex.map { case (e, i) => e.durable(i) }
      .map(Delivered(_))
      .prepend(Source(durables(emitted)).map(Delivered(_)))
      .prepend(Source.single(Recovered))

  "An EventSourcing stage" when {
    "joined with a test event log" must {
      val processor: Flow[Request, Response, NotUsed] =
        EventSourcing(emitterId, 0, requestHandler, eventHandler).join(testEventLog())

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
        EventSourcing(emitterId, 0, requestHandler, eventHandler).join(testEventLog(Seq(Emitted(Incremented(1), emitterId))))

      "first recover state and then consume commands and produce responses" in {
        val commands = Seq(-4, 7).map(Increment)
        val expected = Seq(-3, 4).map(Response)
        Source(commands).via(processor).runWith(Sink.seq).futureValue should be(expected)
      }
    }
    "joined with an Akka Persistence event log" must {
      def processor(persistenceId: String): Flow[Request, Response, NotUsed] =
        EventSourcing(emitterId, 0, requestHandler, eventHandler).join(akkaPersistenceEventLog.flow(persistenceId))

      "consume commands and produce responses" in {
        val persistenceId = "pid-1"
        val commands = Seq(1, -4, 7).map(Increment)
        val expected = Seq(1, -3, 4).map(Response)
        Source(commands).via(processor(persistenceId)).runWith(Sink.seq).futureValue should be(expected)
      }

      "first recover state and then consume commands and produce responses" in {
        val persistenceId = "pid-2"
        Source.single(Emitted(Incremented(1), emitterId)).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
        val commands = Seq(-4, 7).map(Increment)
        val expected = Seq(-3, 4).map(Response)
        Source(commands).via(processor(persistenceId)).runWith(Sink.seq).futureValue should be(expected)
      }

      "first recover state and then consume state dependent command with correct state" in {
        val persistenceId = "pid-3"
        Source.single(Emitted(Incremented(5), emitterId)).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
        Source.single(ClearIfEqualTo(5)).via(processor(persistenceId)).runWith(Sink.seq).futureValue should be(Seq(Response(0)))
      }

      "first recover state and then consume event emitting command followed by state dependent command with correct state" in {
        val persistenceId = "pid-4"
        Source.single(Emitted(Incremented(5), emitterId)).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
        Source(Seq(Increment(0), ClearIfEqualTo(5))).via(processor(persistenceId)).runWith(Sink.seq).futureValue should be(Seq(Response(5), Response(0)))
      }

      "first recover state and then consume event emitting command and produce response" in {
        val persistenceId = "pid-5"
        Source.single(Emitted(Incremented(1), emitterId)).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
        Source.single(Increment(2)).via(processor(persistenceId)).runWith(Sink.seq).futureValue should be(Seq(Response(3)))
      }

      "first recover state and then consume non event emitting command and produce response" in {
        val persistenceId = "pid-6"
        Source.single(Emitted(Incremented(1), emitterId)).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
        Source.single(GetState).via(processor(persistenceId)).runWith(Sink.seq).futureValue should be(Seq(Response(1)))
      }
    }
    "joined with a Kafka event log" must {
      def processor(topicPartition: TopicPartition): Flow[Request, Response, NotUsed] =
        EventSourcing(emitterId, 0, requestHandler, eventHandler).join(kafkaEventLog.flow(topicPartition))

      "consume commands and produce responses" in {
        val topicPartition = new TopicPartition("p-1", 0)
        val commands = Seq(1, -4, 7).map(Increment)
        val expected = Seq(1, -3, 4).map(Response)
        Source(commands).via(processor(topicPartition)).runWith(Sink.seq).futureValue should be(expected)
      }

      "first recover state and then consume commands and produce responses" in {
        val topicPartition = new TopicPartition("p-2", 0)
        Source.single(Emitted(Incremented(1), emitterId)).runWith(kafkaEventLog.sink(topicPartition)).futureValue
        val commands = Seq(-4, 7).map(Increment)
        val expected = Seq(-3, 4).map(Response)
        Source(commands).via(processor(topicPartition)).runWith(Sink.seq).futureValue should be(expected)
      }
    }
  }
}
