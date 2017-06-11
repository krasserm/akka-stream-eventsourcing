# Event sourcing for Akka Streams

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akka-stream-eventsourcing/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/krasserm/akka-stream-eventsourcing.svg?branch=master)](https://travis-ci.org/krasserm/akka-stream-eventsourcing)

## General concept

This project brings to [Akka Streams](http://doc.akka.io/docs/akka/2.5.2/scala/stream/index.html) what [Akka Persistence](http://doc.akka.io/docs/akka/2.5.2/scala/persistence.html) brings to [Akka Actors](http://doc.akka.io/docs/akka/2.5.2/scala/actors.html): persistence via event sourcing. It provides a stateful [`EventSourcing`](https://github.com/krasserm/akka-stream-eventsourcing/blob/master/src/main/scala/com/github/krasserm/ases/EventSourcing.scala) graph stage of type `BidiFlow[REQ, E, E, RES, _]` (simplified) which models the event sourcing message flow. An `EventSourcing` stage therefore 

- consumes and validates a request (command or query)
- produces events (derived from a command and internal state) to be written to an event log
- consumes written events for updating internal state
- produces a response after internal state update

An `EventSourcing` stage maintains current state and uses a request handler for validating requests and generating events and responses:
  
```scala
  import com.github.krasserm.ases.EventSourcing._

  type RequestHandler[S, E, REQ, RES] = (S, REQ) => Emission[S, E, RES]
```

Request handler input is current state and a request, output is an instruction to emit events and/or a response. `emit` instructs the `EventSourcing` stage to emit `events` and call a `responseFactory` with current state after all emitted events have been written and applied to current state:   

```scala
  def emit[S, E, RES](events: Seq[E], responseFactory: S => RES): Emission[S, E, RES]
```

`respond` instructs the `EventSourcing` stage to emit a `response` immediately without emitting any events: 

```scala
  def respond[S, E, RES](response: RES): Emission[S, E, RES]
```

Methods `emit` and `respond` themselves are side-effect free. They only generate `Emission` values that are interpreted by the `EventSourcing` stage. 

For updating internal state the `EventSourcing` stage uses an event handler:
  
```scala
  type EventHandler[S, E] = (S, E) => S
```

Event handler input is current state and a written event, output is updated state. Given definitions of initial state, a request handler and an event handler, an `EventSourcing` stage can be constructed with:
   
```scala
  import akka.stream.scaladsl.BidiFlow
  import com.github.krasserm.ases.EventSourcing

  def initialState: S
  def requestHandler: RequestHandler[S, E, REQ, RES]
  def eventHandler: EventHandler[S, E]

  def eventSourcingStage: BidiFlow[REQ, E, E, RES, _] =
    EventSourcing(initialState, requestHandler, eventHandler)
```
   
Event logs are modeled as `Flow[E, E, _]`. This project provides event log implementations that can use [Akka Persistence journals](http://doc.akka.io/docs/akka/2.5.2/scala/persistence.html#storage-plugins) or [Apache Kafka](http://kafka.apache.org/) as storage backends. [Eventuate event logs](http://rbmhtechnology.github.io/eventuate/architecture.html#event-logs) as storage backends will be supported later. The following example uses the Akka Persistence in-memory journal as storage backend:    
   
```scala
  import akka.stream.scaladsl.Flow
  import com.github.krasserm.ases.log.AkkaPersistenceEventLog

  val provider = new AkkaPersistenceEventLog(journalId = "akka.persistence.journal.inmem")

  def persistenceId: String

  def eventLog: Flow[E, E, _] =
    provider.flow[E](persistenceId)
```

After materialization, an event log emits replayed events to its output port before emitting newly written events that have been received from its input port. To allow `eventSourcingStage` to use `eventLog` for writing and reading events it must `join` the event log:

```scala
  def requestProcessor: Flow[REQ, RES, _] =
    eventSourcingStage.join(eventLog)
```

The result is a stateful, event-sourced request processor of type `Flow[REQ, RES, _]` that processes a request stream. It will only demand new requests from upstream if there is downstream demand for responses and events. Any slowdown in response processing or event writing will back-pressure upstream request producers.

In the same way as `PersistentActor`s in Akka Persistence, request processors provide a consistency boundary around internal state but additionally provide type safety and back-pressure for the whole event sourcing message flow.

The examples presented in this section are a bit simplified for better readability. Take a look at the sources and tests (e.g. [`EventSourcingSpec`](https://github.com/krasserm/akka-stream-eventsourcing/blob/master/src/test/scala/com/github/krasserm/ases/EventSourcingSpec.scala)) for further details.

## Dynamic request processor loading

When applying domain-driven design, consistency boundaries are often around so-called *aggregates*. A request processor that manages a single aggregate should be dynamically loaded and recovered when the first request targeted at that aggregate arrives.
 
This can be achieved with a [`Router`](https://github.com/krasserm/akka-stream-eventsourcing/blob/master/src/main/scala/com/github/krasserm/ases/Router.scala). A router is configured with a function that extracts the aggregate id from a request and another function that creates a request processor from an aggregate id:   

```scala
  import com.github.krasserm.ases.Router

  trait Aggregate[A] {
    def aggregateId(a: A): String
  }

  def requestProcessor(aggregateId: String): Flow[REQ, RES, _] =
    eventSourcingStage.join(provider.flow[E](persistenceId))

  def requestRouter(implicit agg: Aggregate[REQ]): Flow[REQ, RES, _] = {
    Router(req => agg.aggregateId(req), (aggregateId: String) => requestProcessor(aggregateId))
  }
```

A running example is in [`RequestRoutingSpec`](https://github.com/krasserm/akka-stream-eventsourcing/blob/master/src/test/scala/com/github/krasserm/ases/RequestRoutingSpec.scala). Dynamic unloading of request processors (e.g. using an LRU policy) is not supported yet as this requires special support from Akka Streams.

## Request processor collaboration (microservices)

In contrast to `PersistentActor`s, `EventSourcing` stages can form a group by sharing an event log. Within a group, events emitted by one member can be consumed by all members in the group i.e. stages communicate via broadcast using the ordering guarantees of the underlying event log implementation. This feature can be used to implement event-sourced microservices that collaborate via events over a shared event log. 

The following example defines a `requestProcessor` that uses an Apache Kafka topic partition as shared event log. Multiple materializations of `requestProcessor` form a request processor group whose members collaborate over the shared `kafkaTopicPartition`. All members consume events in the same order as a topic partition provides total ordering:

```scala
  import com.github.krasserm.ases.log.KafkaEventLog
  import org.apache.kafka.common.TopicPartition

  def kafkaHost: String
  def kafkaPort: Int
  def kafkaTopicPartition: TopicPartition

  val provider = new KafkaEventLog(kafkaHost, kafkaPort)

  def requestProcessor: Flow[REQ, RES, _] =
    EventSourcing(initialState, requestHandler, eventHandler)
      .join(provider.flow(kafkaTopicPartition))
```

Materializations are not shown here but you can find a running example in [`EventCollaborationSpec`](https://github.com/krasserm/akka-stream-eventsourcing/blob/master/src/test/scala/com/github/krasserm/ases/EventCollaborationSpec.scala) (a replicated event-sourced counter). Collaboration of request processors is comparable to collaboration of `EventsourcedActor`s in [Eventuate](http://rbmhtechnology.github.io/eventuate/) (see [event collaboration](http://rbmhtechnology.github.io/eventuate/architecture.html#event-collaboration) for details).

## Handler switching

Applications can switch request and event handlers as a function of current state by defining request and event handler *providers*: 

```scala
  def initialState: S
  def requestHandlerProvider: S => RequestHandler[S, E, REQ, RES]
  def eventHandlerProvider: S => EventHandler[S, E]

  def eventSourcingStage: BidiFlow[REQ, E, E, RES, _] =
    EventSourcing(initialState, requestHandlerProvider, eventHandlerProvider)
```
   
A request handler provider is called with current state for each request, an event handler provider is called with current state for each written event. This feature will be used later to implement a state machine DSL on top of the current handler API.

## Event logging protocols
 
The examples so far modeled event logs as `Flow[E, E, _]` which is not sufficient for real-world use cases. In order to support atomic batch writes, for example, an event log should have a signature like `Flow[AtomicBatch[E], E, _]`. An `EventSourcing` stage also needs to know when an event log transitions from event replay to live event delivery. This is currently implemented with a [`DeliveryProtocol`](https://github.com/krasserm/akka-stream-eventsourcing/blob/master/src/main/scala/com/github/krasserm/ases/DeliveryProtocol.scala) which requires event logs to be of type `Flow[E, Delivery[E], _]`. The currently implemented event logging protocols are preliminary and will change in the future. 
 
## Project status

In its current state, the project is a prototype that demonstrates the basic ideas how event sourcing and event collaboration could be added to Akka Streams. It can be used for experiments but is not yet ready for production. The prototype should serve as basis for further discussions and evolve to a potential later contribution to Akka, if there is enough interest in the community.    
