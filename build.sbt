
scalaVersion := "2.11.12"

name := "bigdatacapstone"
organization := "rev"
version := "1.0"

//Spark
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8"

// Kafka
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.8.0"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.4.1"
// Spark + Kafka dep spark-streaming-kafka only supports max Scala version 2.11
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"

// Circe (Serializer)
// libraryDependencies += "io.circe" %% "circe-core" % "0.14.1"
// libraryDependencies += "io.circe" %% "circe-generic" % "0.14.1"
// libraryDependencies += "io.circe" %% "circe-parser" % "0.14.1"

