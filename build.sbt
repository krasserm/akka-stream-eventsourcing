lazy val commonSettings = Seq(
  organization := "com.github.krasserm",
  name := "akka-stream-eventsourcing",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.12.2"
)

lazy val dependencies = Seq(
  "com.typesafe.akka" %% "akka-stream"              % Version.Akka ,
  "com.typesafe.akka" %% "akka-stream-kafka"        % "0.16",
  "org.apache.kafka"  %  "kafka-clients"            % Version.Kafka,
  "org.apache.kafka"  %  "kafka-streams"            % Version.Kafka,

  "org.scalatest"     %% "scalatest"                % "3.0.1"      % "test",
  "com.typesafe.akka" %% "akka-stream-testkit"      % Version.Akka % "test",
  "com.typesafe.akka" %% "akka-testkit"             % Version.Akka % "test",
  "net.manub"         %% "scalatest-embedded-kafka" % "0.13.1"     % "test"
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= dependencies)