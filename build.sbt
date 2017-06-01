import de.heikoseeberger.sbtheader.license.Apache2_0

lazy val commonSettings = Seq(
  organization := "com.github.krasserm",
  name := "akka-stream-eventsourcing",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.12.2"
)

lazy val testSettings = Seq(
  parallelExecution in Test := false,
  fork in Test := true
)

lazy val header = Apache2_0("2017", "the original author or authors.")

lazy val headerSettings = Seq(headers := Map(
  "scala" -> header,
  "java" -> header
))

lazy val dependencies = Seq(
  "com.typesafe.akka" %% "akka-persistence"         % Version.Akka ,
  "com.typesafe.akka" %% "akka-stream"              % Version.Akka ,
  "com.typesafe.akka" %% "akka-stream-kafka"        % "0.16",
  "org.apache.kafka"  %  "kafka-clients"            % Version.Kafka,
  "org.apache.kafka"  %  "kafka-streams"            % Version.Kafka,

  "ch.qos.logback"    %  "logback-classic"          % "1.2.3"      % "test",
  "org.slf4j"         %  "log4j-over-slf4j"         % "1.7.25"     % "test",
  "org.scalatest"     %% "scalatest"                % "3.0.1"      % "test",
  "com.typesafe.akka" %% "akka-slf4j"               % Version.Akka % "test",
  "com.typesafe.akka" %% "akka-stream-testkit"      % Version.Akka % "test",
  "com.typesafe.akka" %% "akka-testkit"             % Version.Akka % "test",
  "net.manub"         %% "scalatest-embedded-kafka" % "0.13.1"     % "test" exclude("log4j", "log4j")
)

lazy val root = (project in file("."))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings: _*)
  .settings(testSettings: _*)
  .settings(headerSettings: _*)
  .settings(libraryDependencies ++= dependencies)