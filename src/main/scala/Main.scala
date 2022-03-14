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

<<<<<<< HEAD
  val ryProducerTest = new RYProducerTest
  val ryConsumerTest = new RYConsumer

  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    println("Main app started")
=======
  

  def main(args: Array[String]) = {
    println("Main app started")
    System.setProperty("hadoop.home.dir", "C:\\winutils")
>>>>>>> refs/remotes/origin/kafka/bryan
    //System.setProperty("hadoop.home.dir", "C://hadoop")

    ////Kakfa Main
    //MainConsumerEntry.main() //need two terminals "sbt run" for Kafka entry points
    //MainProducerEntry.main() //need two terminals "sbt run" for Kafka entry points

    ////Producer Main
    MainProducer.startMainProducer()

    ////Consumer Main
<<<<<<< HEAD
    MainConsumer.startMainConsumer()    
    ryConsumerTest.queryConsumer()
=======
    // MainConsumer.startMainConsumer()    
    // ryConsumerTest.queryConsumer()
    //val ryProducerTest = new RYProducerTest
    //val ryConsumerTest = new RYConsumer
>>>>>>> refs/remotes/origin/kafka/bryan
    //ryProducerTest.producerTest()
    //ryConsumerTest.consumerTest()
  }

}
