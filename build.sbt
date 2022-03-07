
scalaVersion := "2.11.12"

name := "P3"
organization := "rev"
version := "1.0"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

//Spark
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8"
<<<<<<< HEAD

//Spark + Kafka integration spark-streaming-kafka only supports max Scala version 2.11
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"
=======
>>>>>>> 7279be2... update
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.8"

// Kafka
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.8.0"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.4.1"



// Circe (Serializer)
// libraryDependencies += "io.circe" %% "circe-core" % "0.14.1"
// libraryDependencies += "io.circe" %% "circe-generic" % "0.14.1"
// libraryDependencies += "io.circe" %% "circe-parser" % "0.14.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10

