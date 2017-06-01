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
import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import com.github.krasserm.ases.log.{KafkaEventLog, KafkaSpec}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, WordSpecLike}

class EventCollaborationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with ScalaFutures with StreamSpec with KafkaSpec {
  import EventSourcingSpec._

  implicit val pc = PatienceConfig(timeout = Span(5, Seconds), interval = Span(10, Millis))

  val kafkaEventLog: KafkaEventLog =
    new log.KafkaEventLog(host, port)

  def processor(topicPartition: TopicPartition): Flow[Request, Response, NotUsed] =
    EventSourcing(0, requestHandler, eventHandler).join(kafkaEventLog.flow(topicPartition))


  "A group of EventSourcing stage" when {
    "joined with a shared event log" must {
      "collaborate via publish-subscribe" in {
        val topicPartition = new TopicPartition("p-1", 0)    // shared topic partition
        val (pub1, sub1) = probes(processor(topicPartition)) // processor 1
        val (pub2, sub2) = probes(processor(topicPartition)) // processor 2

        pub1.sendNext(Increment(3))
        // Both processors receive event but
        // only processor 1 creates response
        sub1.requestNext(Response(3))

        pub2.sendNext(Increment(-4))
        // Both processors receive event but
        // only processor 2 creates response
        sub2.requestNext(Response(-1))
      }
    }
  }
}
