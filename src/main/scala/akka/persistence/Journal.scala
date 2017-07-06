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

package akka.persistence

import akka.actor._
import akka.pattern.ask
import akka.persistence.JournalProtocol._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.Timeout
import akka.{Done, NotUsed}
import com.github.krasserm.ases.{Delivered, Delivery, Recovered}

import scala.collection.immutable.{Seq, VectorBuilder}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._

class Journal(journalId: String)(implicit system: ActorSystem) {
  private val extension = Persistence(system)
  private val journalActor = extension.journalFor(journalId)

  def eventLog(persistenceId: String, fromSequenceNr: Long): Flow[PersistentRepr, Delivery[PersistentRepr], NotUsed] =
    Flow[PersistentRepr].batch(10, Vector(_))(_ :+ _).via(Flow.fromGraph(new EventLog(persistenceId, fromSequenceNr, journalActor)))

  def eventSource(persistenceId: String, fromSequenceNr: Long): Source[Delivery[PersistentRepr], NotUsed] =
    Source.single(Seq.empty).via(Flow.fromGraph(new EventLog(persistenceId, fromSequenceNr, journalActor)))

  def eventSink(persistenceId: String): Sink[PersistentRepr, Future[Done]] =
    eventLog(persistenceId, Long.MaxValue /* only recover sequence number */).toMat(Sink.ignore)(Keep.right)
}

private class EventLog(persistenceId: String, fromSequenceNr: Long, journalActor: ActorRef)(implicit factory: ActorRefFactory)
  extends GraphStage[FlowShape[Seq[PersistentRepr], Delivery[PersistentRepr]]] {

  import EventReplayer.Replayed
  import EventWriter.Written

  private val writer: EventWriter = new EventWriter(persistenceId, journalActor)
  private val replayer: EventReplayer = new EventReplayer(persistenceId, journalActor)

  val in = Inlet[Seq[PersistentRepr]]("EventLogStage.in")
  val out = Outlet[Delivery[PersistentRepr]]("EventLogStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private val writeSuccessCallback = getAsyncCallback(onWriteSuccess)
      private val replaySuccessCallback = getAsyncCallback(onReplaySuccess)
      private val failureCallback = getAsyncCallback(failStage)

      private var completed = false
      private var writing = false
      private var replaying = true
      private var currentSequenceNr: Long = math.max(fromSequenceNr - 1L, 0L)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          writing = true
          writer.write(grab(in).map(_.update(sequenceNr = nextSequenceNr)), currentSequenceNr + 1L).onComplete {
            case Success(written) => writeSuccessCallback.invoke(written)
            case Failure(cause)   => failureCallback.invoke(cause)
          }(materializer.executionContext)
        }

        override def onUpstreamFinish(): Unit =
          if (writing) completed = true else super.onUpstreamFinish()
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (replaying) replayer.replay(currentSequenceNr + 1L, 100).onComplete {
            case Success(replayed) => replaySuccessCallback.invoke(replayed)
            case Failure(cause)    => failureCallback.invoke(cause)
          }(materializer.executionContext) else pull(in)
        }
      })

      private def onReplaySuccess(replayed: Replayed): Unit = {
        currentSequenceNr = replayed.lastSequenceNr
        emitMultiple(out, replayed.events.map(Delivered(_)))
        if (currentSequenceNr >= replayed.currentSequenceNr) {
          // Needed if highest sequence number in journal is
          // less than (fromSequenceNr - 1L).
          currentSequenceNr = replayed.currentSequenceNr
          replaying = false
          emit(out, Recovered)
        }
      }

      private def onWriteSuccess(written: Written): Unit = {
        writing = false
        emitMultiple(out, written.events.map(Delivered(_)))
        if (completed) completeStage()
      }

      private def nextSequenceNr: Long = {
        currentSequenceNr = currentSequenceNr + 1L
        currentSequenceNr
      }
    }
}

private object EventWriter {
  case class Write(events: Seq[PersistentRepr], from: Long)
  case class Written(events: Seq[PersistentRepr])
}

private class EventWriter(persistenceId: String, journalActor: ActorRef)(implicit factory: ActorRefFactory) {
  import EventWriter._

  private implicit val timeout = Timeout(10.seconds) // TODO: make configurable

  def write(events: Seq[PersistentRepr], from: Long): Future[Written] =
    factory.actorOf(Props(new EventWriterActor(persistenceId, journalActor))).ask(Write(events, from)).mapTo[Written]
}

private class EventWriterActor(persistenceId: String, journalActor: ActorRef) extends Actor {
  import EventWriter._

  private var written = 0
  private var events: Seq[PersistentRepr] = Seq()
  private var sdr: ActorRef = _

  override def receive: Receive = {
    case Write(Seq(), from) =>
      sender() ! Written(Seq())
      context.stop(self)
    case Write(es, from) =>
      sdr = sender()
      events = es
      journalActor ! WriteMessages(Seq(AtomicWrite(es)), self, 0)
    case WriteMessagesFailed(cause) =>
      sdr ! Status.Failure(cause)
      context.stop(self)
    case WriteMessagesSuccessful =>
      sdr ! Written(events)
    case WriteMessageSuccess(_, _) =>
      written = written + 1
      if (written == events.size) context.stop(self)
  }
}

private object EventReplayer {
  case class Replay(from: Long, max: Long)
  case class Replayed(events: Seq[PersistentRepr], currentSequenceNr: Long) {
    def lastSequenceNr: Long = if (events.isEmpty) currentSequenceNr else events.last.sequenceNr
  }
}

private class EventReplayer(persistenceId: String, journalActor: ActorRef)(implicit factory: ActorRefFactory) {
  import EventReplayer._

  private implicit val timeout = Timeout(10.seconds) // TODO: make configurable

  def replay(from: Long, max: Long): Future[Replayed] =
    factory.actorOf(Props(new EventReplayerActor(persistenceId, journalActor))).ask(Replay(from, max)).mapTo[Replayed]
}

private class EventReplayerActor(persistenceId: String, journal: ActorRef) extends Actor {
  import EventReplayer._

  private val builder: VectorBuilder[PersistentRepr] = new VectorBuilder[PersistentRepr]
  private var sdr: ActorRef = _

  override def receive: Receive = {
    case Replay(from, max) =>
      sdr = context.sender()
      journal ! ReplayMessages(from, Long.MaxValue, max, persistenceId, self)
    case ReplayedMessage(p) =>
      builder += p
    case ReplayMessagesFailure(cause) =>
      sdr ! Status.Failure(cause)
      context.stop(self)
    case RecoverySuccess(snr) =>
      sdr ! Replayed(builder.result(), snr)
      context.stop(self)
  }
}


