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


object Main {
  println("Main app started")

  // may need to import team members's SPARK_HOME Path for .setSparkHome(/*SPARK_HOME_PATH*/) for each individual member's branches
  val sconf = new SparkConf().setMaster("local[*]").setAppName("P3").setSparkHome("C:\\Spark")
  // SparkContext (sc) is main entrypoint for Spark API
  val sc   = new SparkContext(sconf)
  // StreamingContext (ssc) is main entrypoint for Spark Streaming API, built on top of SparkContext (sc)
  val ssc  = new StreamingContext(sc, Seconds(2))
  // Spark log level set to not print INFO lines, accessed through the SparkContext (sc) "The associated SparkContext [sc beneath ssc] can be accessed using ssc.sparkContext ~= sc"
  ssc.sparkContext.setLogLevel("ERROR")
  // Spark SQL context, SparkSession
  //val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").enableHiveSupport().getOrCreate()

  
  def kafkaFunction(): Unit = {
    val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topicA", "topicB")
  val stream = KafkaUtils.createDirectStream[String, String](
    // ssc is StreamingContext
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.map(record => (record.key, record.value))
  }

}

