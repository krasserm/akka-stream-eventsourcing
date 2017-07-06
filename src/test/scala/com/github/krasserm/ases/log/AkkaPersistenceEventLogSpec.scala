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
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.github.krasserm.ases._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.collection.immutable.Seq

class AkkaPersistenceEventLogSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with ScalaFutures with BeforeAndAfterEach with StreamSpec {
  val akkaPersistenceEventLog: AkkaPersistenceEventLog = new AkkaPersistenceEventLog(journalId = "akka.persistence.journal.inmem")

  private var counter: Int = 0

  override def beforeEach(): Unit =
    nextPersistenceId()

  def persistenceId: String =
    counter.toString

  def nextPersistenceId(): String = {
    counter += 1
    persistenceId
  }

  "An Akka Persistence event log" must {
    "provide a sink for writing events and a source for delivering replayed events" in {
      val events = Seq("a", "b", "c").map(Emitted(_, emitterId))
      val expected = durables(events, offset = 1).map(Delivered(_)) :+ Recovered

      Source(events).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
      akkaPersistenceEventLog.source[String](persistenceId).runWith(Sink.seq).futureValue should be(expected)
    }
    "provide a flow with an input port for writing events and and output port for delivering replayed and live events" in {
      val events1 = Seq("a", "b", "c").map(Emitted(_, emitterId))
      val events2 = Seq("d", "e", "f").map(Emitted(_, emitterId))
      val expected = (durables(events1, offset = 1).map(Delivered(_)) :+ Recovered) ++ durables(events2, offset = 4).map(Delivered(_))

      Source(events1).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
      Source(events2).via(akkaPersistenceEventLog.flow(persistenceId)).runWith(Sink.seq).futureValue should be(expected)
    }
    "provide a source that only delivers events of compatible types" in {
      val events = Seq("a", "b", 1, 2).map(Emitted(_, emitterId))
      val expected = durables(events, offset = 1).drop(2).map(Delivered(_)) :+ Recovered

      Source(events).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
      akkaPersistenceEventLog.source[Int](persistenceId).runWith(Sink.seq).futureValue should be(expected)
    }
    "provide a source that supports replay from an application-defined sequence number" in {
      val events = Seq("a", "b", "c").map(Emitted(_, emitterId))

      def expected(fromSequenceNr: Long): Seq[Delivery[Durable[String]]] =
        durables(events.drop(fromSequenceNr.toInt - 1), offset = math.max(fromSequenceNr, 1)).map(Delivered(_)) :+ Recovered

      Source(events).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
      Seq(-1L, 0L, 1L, 2L, 3L, 4L).foreach { fromSequenceNr =>
        akkaPersistenceEventLog.source[String](persistenceId, fromSequenceNr).runWith(Sink.seq).futureValue should be(expected(fromSequenceNr))
      }
    }
    "provide a flow that supports replay from an application-defined sequence number" in {
      val events1 = Seq("a", "b", "c").map(Emitted(_, emitterId))
      val events2 = Seq("d", "e", "f").map(Emitted(_, emitterId))

      def expected(fromSequenceNr: Long): Seq[Delivery[Durable[String]]] =
        (durables(events1.drop(fromSequenceNr.toInt - 1), offset = math.max(fromSequenceNr, 1)).map(Delivered(_)) :+ Recovered) ++ durables(events2, offset = 4).map(Delivered(_))

      Seq(-1L, 0L, 1L, 2L, 3L, 4L).foreach { fromSequenceNr =>
        val persistenceId = nextPersistenceId()
        Source(events1).runWith(akkaPersistenceEventLog.sink(persistenceId)).futureValue
        Source(events2).via(akkaPersistenceEventLog.flow(persistenceId, fromSequenceNr = fromSequenceNr)).runWith(Sink.seq).futureValue should be(expected(fromSequenceNr))
      }
    }
  }
}
