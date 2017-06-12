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

import java.util
import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.serialization.SerializationExtension
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.{BidiFlow, Flow, Keep, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.{Done, NotUsed}
import com.github.krasserm.ases._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
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
  private val kafkaSource = new KafkaSource(host, port)
  private val kafkaSink = new KafkaSink(host, port)
  private val kafkaCodec = new KafkaCodec()

  /**
    * Creates a flow representing the event log identified by `topicPartition`. Input
    * events are written to the partition and emitted as output events after they have
    * been re-consumed from that partition. Events that already exist in the given topic
    * partition at materialization time are emitted before any newly written events are
    * emitted (recovery phase).
    *
    * During recovery, events are emitted as [[Delivered]] messages, recovery completion
    * is signaled as [[Recovered]] message, both subtypes of [[Delivery]]. After recovery,
    * events are again emitted as [[Delivered]] messages.
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
  def flow[A: ClassTag](topicPartition: TopicPartition): Flow[Emitted[A], Delivery[Durable[A]], NotUsed] =
    kafkaCodec.apply[A](topicPartition).join(Flow.fromSinkAndSource(kafkaSink.apply, kafkaSource(topicPartition)))

  /**
    * Creates a source that emits all events of the event log identified by `topicPartition`.
    * The source not only emits replayed events (recovery phase) but also emits events that
    * have been newly written by others to given topic partition.
    *
    * During recovery, events are emitted as [[Delivered]] messages, recovery completion
    * is signaled as [[Recovered]] message, both subtypes of [[Delivery]]. After recovery,
    * events are again emitted as [[Delivered]] messages.
    *
    * @param topicPartition topic partition of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def source[A: ClassTag](topicPartition: TopicPartition): Source[Delivery[Durable[A]], NotUsed] =
    kafkaSource(topicPartition).via(kafkaCodec.decoder)

  /**
    * Creates a sink that writes events to the event log identified by `topicPartition`.
    *
    * @param topicPartition topic partition of the event log.
    * @tparam A event type. Only events that are instances of given type are
    *           emitted by the source.
    */
  def sink[A](topicPartition: TopicPartition): Sink[Emitted[A], Future[Done]] =
    kafkaCodec.encoder(topicPartition).toMat(kafkaSink.apply)(Keep.right)
}

private object KafkaCodec {
  object ConsumerRecordExtractor {
    def unapply[A](cr: ConsumerRecord[String, A]): Option[(A, Long)] =
      Some(cr.value, cr.offset)
  }
}

private class KafkaCodec(implicit system: ActorSystem) {
  def apply[A: ClassTag](topicPartition: TopicPartition): BidiFlow[Emitted[A], ProducerRecord[String, Emitted[Any]], Delivery[ConsumerRecord[String, Emitted[Any]]], Delivery[Durable[A]], NotUsed] =
    BidiFlow.fromFlows(encoder(topicPartition), decoder)

  def decoder[A: ClassTag]: Flow[Delivery[ConsumerRecord[String, Emitted[Any]]], Delivery[Durable[A]], NotUsed] =
    Flow[Delivery[ConsumerRecord[String, Emitted[Any]]]].collect {
      case Delivered(KafkaCodec.ConsumerRecordExtractor(Emitted(e: A, emitterId, emissionUuid), sequenceNr)) =>
        Delivered(Durable(e, emitterId, emissionUuid, sequenceNr))
      case Recovered =>
        Recovered
    }

  def encoder[A](topicPartition: TopicPartition): Flow[Emitted[A], ProducerRecord[String, Emitted[A]], NotUsed] =
    Flow[Emitted[A]].map(emitted => new ProducerRecord(topicPartition.topic, topicPartition.partition, emitted.emitterId, emitted))

  private def eventType[A: ClassTag]: PartialFunction[Any, A] = {
    case event: A => event
  }
}

private class KafkaSerialization(implicit val system: ActorSystem) extends Serializer[Emitted[Any]] with Deserializer[Emitted[Any]] {
  val serialization = SerializationExtension(system)

  override def close(): Unit = ()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: Emitted[Any]): Array[Byte] =
    serialization.serialize(data).get

  override def deserialize(topic: String, data: Array[Byte]): Emitted[Any] =
    serialization.deserialize(data, classOf[Emitted[Any]]).get
}

private class KafkaSink(host: String, port: Int)(implicit system: ActorSystem) {
  val producerSettings: ProducerSettings[String, Emitted[Any]] =
    ProducerSettings(system, new StringSerializer, new KafkaSerialization)
      .withBootstrapServers(s"$host:$port")

  private val producer: KafkaProducer[String, Emitted[Any]] =
    producerSettings.createKafkaProducer()

  def apply: Sink[ProducerRecord[String, Emitted[Any]], Future[Done]] =
    Producer.plainSink(producerSettings, producer)
}

private class KafkaSource(host: String, port: Int)(implicit system: ActorSystem) {
  private val kafkaMetadata: KafkaMetadata[String, Emitted[Any]] =
    new KafkaMetadata(consumerSettings)

  private def consumerSettings: ConsumerSettings[String, Emitted[Any]] =
    ConsumerSettings(system, new StringDeserializer, new KafkaSerialization)
      .withBootstrapServers(s"$host:$port")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withGroupId(UUID.randomUUID().toString)

  def apply(topicPartition: TopicPartition): Source[Delivery[ConsumerRecord[String, Emitted[Any]]], NotUsed] =
    kafkaMetadata.endOffset(topicPartition).flatMapConcat { endOffset =>
      to(topicPartition, endOffset - 1L)
        .map(Delivered(_))
        .concat(Source.single(Recovered))
        .concat(from(topicPartition, endOffset).map(Delivered(_)))
    }

  def from(topicPartition: TopicPartition, fromOffset: Long /* inclusive */): Source[ConsumerRecord[String, Emitted[Any]], NotUsed] =
    Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(topicPartition, fromOffset))
      .mapMaterializedValue(_ => NotUsed)

  def to(topicPartition: TopicPartition, toOffset: Long /* inclusive */): Source[ConsumerRecord[String, Emitted[Any]], NotUsed] =
    if (toOffset < 0L) Source.empty else Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(topicPartition, 0L))
      .mapMaterializedValue(_ => NotUsed)
      .takeWhile(_.offset < toOffset, inclusive = true)
}

private class KafkaMetadata[K, V](consumerSettings: ConsumerSettings[K, V])(implicit system: ActorSystem) {
  def offset(f: KafkaConsumer[K, V] => Long): Source[Long, NotUsed] =
    Source.fromGraph(new KafkaMetadataSource(consumerSettings)(f))

  def endOffset(topicPartition: TopicPartition): Source[Long, NotUsed] =
    offset(endOffset(_, topicPartition))

  private def endOffset(consumer: KafkaConsumer[K, V], topicPartition: TopicPartition): Long =
    consumer.endOffsets(Seq(topicPartition).asJava).get(topicPartition) match {
      case null   => 0L
      case offset => offset.toLong
    }
}

private class KafkaMetadataSource[K, V, R](consumerSettings: ConsumerSettings[K, V])(f: KafkaConsumer[K, V] ⇒ R) extends GraphStage[SourceShape[R]] {
  val out: Outlet[R] = Outlet("KafkaMetadataSource.out")

  override val shape: SourceShape[R] = SourceShape(out)

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
