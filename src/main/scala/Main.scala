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
import org.apache.spark.sql.DataFrameWriter

// Kafka branch
import kafkapack._
// Producer branch
import producerpack._
// Consumer branch
import consumerpack._
// Spark Contexts
import contextpack._

object Main {
  def main(args: Array[String]) = {
    println("Main app started")
    //System.setProperty("hadoop.home.dir", "C://hadoop")
    
    ////Kafka main
    ////MainConsumer consumer works
    //MainConsumer.startMainConsumer()

    //SparkStreamingContext.startSparkStreamingContext()
    TestProducer.setProducer()
    
    // Producer main
    //MainProducer.startMainProducer()

    //MainProducer.startMainProducer()
    //MainConsumer.startMainConsumer()

    //TestProducer.setProducer()
    
    // Spark Structured Streaming + Kafka
    //StructRead.subscribe()
    //StructWrite.send("topic1", Array("product_id", "product_category"))

    //SparkStreamingContext.startSparkStreamingContext()

  }
}