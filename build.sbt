
scalaVersion := "2.11.12"
//scalaVersion := "2.10.4"


name := "P3"
organization := "rev"
version := "1.0"



// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"



// libraryDependencies ++= Seq(
//   "org.apache.spark" % "spark-streaming" % "1.4.1",
//   //"org.apache.spark" % "spark-streaming-kafka" % "1.4.1",         // kafka
//   "org.apache.hbase" % "hbase" % "0.92.1",
//   "org.apache.hadoop" % "hadoop-core" % "1.0.2",
//   "org.apache.spark" % "spark-mllib" % "1.3.0"
// )

// mergeStrategy in assembly := {
//   case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
//   case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
//   case "log4j.properties"                                  => MergeStrategy.discard
//   case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
//   case "reference.conf"                                    => MergeStrategy.concat
//   case _                                                   => MergeStrategy.first
// }

//Spark
 libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.8"
 libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8"
 libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.8"



//Kafka
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.8.0"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.4.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka_2.10" % "2.4.1"


//Akka
//libraryDependencies += "org.apache.bahir" %% "spark-streaming-akka" % "2.4.0-SNAPSHOT"







// Circe (Serializer)
// libraryDependencies += "io.circe" %% "circe-core" % "0.14.1"
// libraryDependencies += "io.circe" %% "circe-generic" % "0.14.1"
// libraryDependencies += "io.circe" %% "circe-parser" % "0.14.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10

