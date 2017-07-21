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

package com.github.krasserm.ases.serializer

import java.io.NotSerializableException

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import com.github.krasserm.ases.Emitted
import com.github.krasserm.ases.serializer.EmittedFormatOuterClass.EmittedFormat

class EmittedSerializer(system: ExtendedActorSystem) extends Serializer {

  private val EmittedClass = classOf[Emitted[_]]
  private val payloadSerializer = new PayloadSerializer(system)

  override def identifier: Int = 17406883

  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case emitted: Emitted[_] =>
      emittedFormat(emitted).toByteArray
    case _ =>
      throw new IllegalArgumentException(s"Invalid object of type '${o.getClass}' supplied to serializer [id = '$identifier']")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case Some(`EmittedClass`) =>
      emitted(EmittedFormat.parseFrom(bytes))
    case None =>
      emitted(EmittedFormat.parseFrom(bytes))
    case _ =>
      throw new NotSerializableException(s"Unknown manifest '$manifest' supplied to serializer [id = '$identifier']")
  }

  private def emittedFormat(emitted: Emitted[Any]): EmittedFormat = {
    EmittedFormat.newBuilder()
      .setEvent(payloadSerializer.payloadFormatBuilder(emitted.event.asInstanceOf[AnyRef]))
      .setEmitterId(emitted.emitterId)
      .setEmissionUuid(emitted.emissionUuid)
      .build()
  }

  private def emitted(format: EmittedFormat): Emitted[_] = {
    Emitted(
      payloadSerializer.payload(format.getEvent),
      format.getEmitterId,
      format.getEmissionUuid
    )
  }
 }
