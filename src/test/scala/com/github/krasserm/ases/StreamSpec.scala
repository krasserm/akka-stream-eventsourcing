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

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.immutable.Seq

trait StreamSpec extends StopSystemAfterAll { this: TestKit with Suite =>
  implicit val materializer = ActorMaterializer()

  val emitterId = "emitter"

  override def afterAll(): Unit = {
    materializer.shutdown()
    super.afterAll()
  }

  def probes[I, O, M](flow: Flow[I, O, M]): (TestPublisher.Probe[I], TestSubscriber.Probe[O]) =
    TestSource.probe[I].viaMat(flow)(Keep.left).toMat(TestSink.probe[O])(Keep.both).run()

  def durables[A](emitted: Seq[Emitted[A]], offset: Long = 0L): Seq[Durable[A]] =
    emitted.zipWithIndex.map { case (e, i) => e.durable(i + offset) }
}
