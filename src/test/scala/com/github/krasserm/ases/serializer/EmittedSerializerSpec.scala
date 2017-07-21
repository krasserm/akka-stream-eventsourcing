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

import akka.actor.ActorSystem
import akka.serialization.{SerializationExtension, Serializer, SerializerWithStringManifest}
import akka.testkit.TestKit
import com.github.krasserm.ases.{Emitted, SpecWords, StopSystemAfterAll}
import com.google.protobuf
import org.scalatest.{MustMatchers, WordSpecLike}

object EmittedSerializerSpec {

  case class StringManifestEvent(id: String)

  case class ClassManifestEvent(id: String)

  case class EmptyManifestEvent(id: String)

  case class PlainEvent(id: String)

  class StringManifestEventSerializer extends SerializerWithStringManifest {
    val Manifest = "v1.Event"

    override def identifier: Int = 11111101

    override def manifest(o: AnyRef): String = Manifest

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case e: StringManifestEvent => e.id.getBytes
      case _ => throw new IllegalArgumentException(s"Object of type '${o.getClass}' not supported")
    }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
      case Manifest => StringManifestEvent(new String(bytes))
      case _ => throw new IllegalArgumentException(s"Object for manifest '$manifest' not supported")
    }
  }

  class ClassManifestEventSerializer extends Serializer {
    val ClassManifest = classOf[ClassManifestEvent]

    override def identifier: Int = 11111102

    override def includeManifest: Boolean = true

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case e: ClassManifestEvent => e.id.getBytes
      case _ => throw new IllegalArgumentException(s"Object of type '${o.getClass}' not supported")
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
      case Some(`ClassManifest`) => ClassManifestEvent(new String(bytes))
      case None => throw new IllegalArgumentException(s"No class manifest provided")
      case _ => throw new IllegalArgumentException(s"Object for manifest '$manifest' not supported")
    }
  }

  class EmptyManifestEventSerializer extends Serializer {

    override def identifier: Int = 11111103

    override def includeManifest: Boolean = false

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case e: EmptyManifestEvent => e.id.getBytes
      case _ => throw new IllegalArgumentException(s"Object of type '${o.getClass}' not supported")
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef =
      EmptyManifestEvent(new String(bytes))
  }
}

class EmittedSerializerSpec extends TestKit(ActorSystem("test")) with WordSpecLike with MustMatchers with SpecWords with StopSystemAfterAll {

  import EmittedSerializerSpec._

  val serialization = SerializationExtension(system)
  val emittedSerializer = serialization.serializerFor(classOf[Emitted[_]])

  def emitted[A](event: A): Emitted[A] =
    Emitted(event, "emitter-1")

  def serialize[A](emitted: Emitted[A]): Array[Byte] =
    emittedSerializer.toBinary(emitted)

  def deserialize[A](bytes: Array[Byte]): Emitted[A] =
    emittedSerializer.fromBinary(bytes, classOf[Emitted[_]]).asInstanceOf[Emitted[A]]

  def emittedSerializations[A](event: A): Unit = {
    "serialize and deserialize the Emitted event with the given payload" in {
      val original = emitted(event)

      val bytes = serialize(original)
      val deserialized = deserialize(bytes)

      deserialized mustBe original
    }
  }

  "An EmittedSerializer" when {
    "serializing an object" that is {
      "an Emitted event" which {
        "contains an event payload assigned to a serializer" which isA {
          "SerializerWithStringManifest" must {
            behave like emittedSerializations(StringManifestEvent("ev-1"))
          }
          "Serializer with class-manifest" must {
            behave like emittedSerializations(ClassManifestEvent("ev-1"))
          }
          "Serializer with an empty manifest" must {
            behave like emittedSerializations(EmptyManifestEvent("ev-1"))
          }
          "JavaSerializer" must {
            behave like emittedSerializations(PlainEvent("ev-1"))
          }
        }
      }
      "an invalid object" must {
        "throw an IllegalArgumentException" in {
          intercept[IllegalArgumentException] {
            emittedSerializer.toBinary("invalid-object")
          }
        }
      }
    }
    "deserializing a byte-array" that contains {
      "an invalid binary representation" must {
        "throw a NotSerializableException" in {
          intercept[protobuf.InvalidProtocolBufferException] {
            emittedSerializer.fromBinary("invalid-binary-representation".getBytes, classOf[Emitted[_]])
          }
        }
      }
      "a valid binary representation" when invokedWith {
        val original = emitted(StringManifestEvent("ev-1"))

        "an empty manifest" must {
          "deserialize the Emitted event" in {
            val deserialized = emittedSerializer.fromBinary(serialize(original), manifest = None)

            deserialized mustBe original
          }
        }
        "an unsupported manifest" must {
          "throw a NotSerializableException" in {
            intercept[NotSerializableException] {
              emittedSerializer.fromBinary(serialize(original), manifest = Some(classOf[String]))
            }
          }
        }
      }
    }
  }
}
