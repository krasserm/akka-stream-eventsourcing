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

import akka.actor.ExtendedActorSystem
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import com.github.krasserm.ases.serializer.PayloadFormatOuterClass.PayloadFormat
import com.google.protobuf.ByteString

import scala.util.Try

class PayloadSerializer(system: ExtendedActorSystem) {

  def payloadFormatBuilder(payload: AnyRef): PayloadFormat.Builder = {
    val serializer = SerializationExtension(system).findSerializerFor(payload)
    val builder = PayloadFormat.newBuilder()

    if (serializer.includeManifest) {
      val (isStringManifest, manifest) = serializer match {
        case s: SerializerWithStringManifest => (true, s.manifest(payload))
        case _ => (false, payload.getClass.getName)
      }
      builder.setIsStringManifest(isStringManifest)
      builder.setPayloadManifest(manifest)
    }
    builder.setSerializerId(serializer.identifier)
    builder.setPayload(ByteString.copyFrom(serializer.toBinary(payload)))
  }

  def payload(payloadFormat: PayloadFormat): AnyRef = {
    val payload = if (payloadFormat.getIsStringManifest)
      payloadFromStringManifest(payloadFormat)
    else if (payloadFormat.getPayloadManifest.nonEmpty)
      payloadFromClassManifest(payloadFormat)
    else
      payloadFromEmptyManifest(payloadFormat)

    payload.get
  }

  private def payloadFromStringManifest(payloadFormat: PayloadFormat): Try[AnyRef] = {
    SerializationExtension(system).deserialize(
      payloadFormat.getPayload.toByteArray,
      payloadFormat.getSerializerId,
      payloadFormat.getPayloadManifest
    )
  }

  private def payloadFromClassManifest(payloadFormat: PayloadFormat): Try[AnyRef]  = {
    val manifestClass = system.dynamicAccess.getClassFor[AnyRef](payloadFormat.getPayloadManifest).get
    SerializationExtension(system).deserialize(
      payloadFormat.getPayload.toByteArray,
      payloadFormat.getSerializerId,
      Some(manifestClass)
    )
  }

  private def payloadFromEmptyManifest(payloadFormat: PayloadFormat): Try[AnyRef]  = {
    SerializationExtension(system).deserialize(
      payloadFormat.getPayload.toByteArray,
      payloadFormat.getSerializerId,
      None
    )
  }
}