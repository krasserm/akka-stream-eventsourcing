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

import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.serialization.SerializationExtension
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.{Done, NotUsed}
import com.github.krasserm.ases.DeliveryProtocol.{Delivered, Delivery, Recovered}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Akka Stream API for events logs managed by Apache Kafka. An individual
  * event log is identified by `topicPartition`.
  *
  * '''This implementation is for testing only!''' It does not prevent or
  * compensate duplicate or re-ordered writes to topic partitions during
  * failures. Also, event retention time is assumed to be infinite.
  *
  * @param host Kafka (bootstrap server) host.
  * @param port Kafka (bootstrap server) port.
  */
class KafkaEventLog(host: String, port: Int)(implicit system: ActorSystem) {
  private val serialization = SerializationExtension(system)

  private def consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(s"$host:$port")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId(UUID.randomUUID().toString)

  private val producerSettings: ProducerSettings[String, Array[Byte]] =
    ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(s"$host:$port")

  private val producer: KafkaProducer[String, Array[Byte]] =
    producerSettings.createKafkaProducer()

  /**
    * Creates a flow representing the event log identified by `topicPartition`. Input
    * events are written to the partition and emitted as output events after they have
    * been re-consumed from that partition. Events that already exist in the given topic
    * partition at materialization time are emitted before any newly written events are
    * emitted (recovery phase).
    *
    * During recovery, events are emitted as [[com.github.krasserm.ases.DeliveryProtocol.Delivered Delivered]]
    * messages, recovery completion is signaled as [[com.github.krasserm.ases.DeliveryProtocol.Recovered Recovered]]
    * message, both subtypes of [[com.github.krasserm.ases.DeliveryProtocol.Delivery Delivery]]. After recovery,
    * events are again emitted as [[com.github.krasserm.ases.DeliveryProtocol.Delivered Delivered]] messages.
    *
    * Applications may create multiple materialized instances of flows writing to and consuming
    * from the same topic partition. In this case, an event written by one instance is consumed
    * by this and all other instances (collaboration via broadcast). All collaborators on a topic
    * partition see events in the same order.
    *
    * @param topicPartition topic partition of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the flow.
    */
  def flow[A: ClassTag](topicPartition: TopicPartition): Flow[A, Delivery[A], NotUsed] =
    Flow.fromSinkAndSource(sink(topicPartition), liveSource(topicPartition))

  /**
    * Creates a source that emits all events of the event log identified by `topicPartition`.
    * The source not only emits replayed events (recovery phase) but also emits events that
    * have been newly written by others to given topic partition.
    *
    * During recovery, events are emitted as [[com.github.krasserm.ases.DeliveryProtocol.Delivered Delivered]]
    * messages, recovery completion is signaled as [[com.github.krasserm.ases.DeliveryProtocol.Recovered Recovered]]
    * message, both subtypes of [[com.github.krasserm.ases.DeliveryProtocol.Delivery Delivery]]. After recovery,
    * events are again emitted as [[com.github.krasserm.ases.DeliveryProtocol.Delivered Delivered]] messages.
    *
    * @param topicPartition topic partition of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def liveSource[A: ClassTag](topicPartition: TopicPartition): Source[Delivery[A], NotUsed] =
    KafkaMetadata.endOffset(consumerSettings, topicPartition).flatMapConcat { endOffset =>
      sourceTo(topicPartition, endOffset - 1L)
        .map(Delivered(_))
        .concat(Source.single(Recovered))
        .concat(sourceFrom(topicPartition, endOffset).map(Delivered(_)))
    }

  /**
    * Creates a source that replays events of the event log identified by `topicPartition`.
    * The source completes when the end of the event log at materialization time is reached
    * (i.e. it is not a live source in contrast to the output of [[flow]] or [[liveSource]]).
    *
    * @param topicPartition topic partition of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def source[A: ClassTag](topicPartition: TopicPartition): Source[A, NotUsed] =
    liveSource(topicPartition).via(replayed)

  /**
    * Creates a sink that writes events to the event log identified by `topicPartition`.
    *
    * @param topicPartition topic partition of the event log.
    */
  def sink[A](topicPartition: TopicPartition): Sink[A, Future[Done]] =
    Flow[A]
      .map(serialize)
      .map(eventBytes => new ProducerRecord(topicPartition.topic, topicPartition.partition, "", eventBytes))
      .toMat(Producer.plainSink(producerSettings, producer))(Keep.right)

  private def sourceFrom[A: ClassTag](topicPartition: TopicPartition, fromOffset: Long /* inclusive */): Source[A, NotUsed] =
    Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(topicPartition, fromOffset))
      .mapMaterializedValue(_ => NotUsed)
      .map(cr => deserialize(cr.value))
      .collect(eventType[A])

  private def sourceTo[A: ClassTag](topicPartition: TopicPartition, toOffset: Long /* inclusive */): Source[A, NotUsed] =
    if (toOffset < 0L) Source.empty else Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(topicPartition, 0L))
      .mapMaterializedValue(_ => NotUsed)
      .takeWhile(_.offset < toOffset, inclusive = true)
      .map(cr => deserialize(cr.value))
      .collect(eventType[A])

  private def serialize(event: Any): Array[Byte] =
    serialization.serialize(event.asInstanceOf[AnyRef]).get

  private def deserialize(eventBytes: Array[Byte]): Any =
    serialization.deserialize(eventBytes, classOf[Serializable]).get

  private def eventType[A: ClassTag]: PartialFunction[Any, A] = {
    case event: A => event
  }
}

private object KafkaMetadata {
  def offset[K, V](consumerSettings: ConsumerSettings[K, V], f: KafkaConsumer[K, V] => Long): Source[Long, NotUsed] =
    Source.fromGraph(new KafkaMetadata(consumerSettings)(f))

  def endOffset[K, V](consumerSettings: ConsumerSettings[K, V], topicPartition: TopicPartition): Source[Long, NotUsed] =
    offset[K, V](consumerSettings, endOffset(_, topicPartition))

  private def endOffset[K, V](consumer: KafkaConsumer[K, V], topicPartition: TopicPartition): Long =
    consumer.endOffsets(Seq(topicPartition).asJava).get(topicPartition) match {
      case null   => 0L
      case offset => offset.toLong
    }
}

private class KafkaMetadata[K, V, R](consumerSettings: ConsumerSettings[K, V])(f: KafkaConsumer[K, V] ⇒ R)
  extends GraphStage[SourceShape[R]] {
  val out: Outlet[R] =
    Outlet("KafkaMetadata.out")

  override val shape: SourceShape[R] =
    SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var consumer: KafkaConsumer[K, V] = _

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          push(out, f(consumer))
      })

      override def preStart(): Unit =
        consumer = consumerSettings.createKafkaConsumer()

      override def postStop(): Unit =
        consumer.close()
    }
}
