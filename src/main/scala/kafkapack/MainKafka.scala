package kafkapack

// Kafka deps
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
// Spark deps
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._

import contextpack._

object MainKafka {
    
  def startMainKafka(): Unit = {

    println("Main Kafka started...")
    ProducerStreaming.streamFromSource("topic1")
    
  }
}
