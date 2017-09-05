enablePlugins(ProtobufPlugin)

lazy val commonSettings = Seq(
  organization := "com.github.krasserm",
  name := "akka-stream-eventsourcing",
  startYear := Some(2017),
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.12.3",
  licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")),
  headerLicense := Some(HeaderLicense.ALv2("2017", "the original author or authors."))
)

lazy val testSettings = Seq(
  parallelExecution in Test := false,
  fork in Test := true
)

lazy val protocSettings: Seq[Setting[_]] = Seq(
  version in ProtobufConfig := Version.Protobuf,
  protobufRunProtoc in ProtobufConfig := (args => com.github.os72.protocjar.Protoc.runProtoc("-v340" +: args.toArray))
)

lazy val dependencies = Seq(
  "com.typesafe.akka" %% "akka-persistence"         % Version.Akka ,
  "com.typesafe.akka" %% "akka-stream"              % Version.Akka ,
  "com.typesafe.akka" %% "akka-stream-kafka"        % "0.17",
  "org.apache.kafka"  %  "kafka-clients"            % Version.Kafka,
  "org.apache.kafka"  %  "kafka-streams"            % Version.Kafka,

  "ch.qos.logback"    %  "logback-classic"          % "1.2.3"      % "test",
  "org.slf4j"         %  "log4j-over-slf4j"         % "1.7.25"     % "test",
  "org.scalatest"     %% "scalatest"                % "3.0.4"      % "test",
  "com.typesafe.akka" %% "akka-slf4j"               % Version.Akka % "test",
  "com.typesafe.akka" %% "akka-stream-testkit"      % Version.Akka % "test",
  "com.typesafe.akka" %% "akka-testkit"             % Version.Akka % "test",
  "net.manub"         %% "scalatest-embedded-kafka" % "0.15.1"     % "test" exclude("log4j", "log4j")
)

lazy val root = (project in file("."))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings: _*)
  .settings(testSettings: _*)
  .settings(protocSettings: _*)
  .settings(libraryDependencies ++= dependencies)
