package kafkapack
//import _root_.kafka.serializer.StringDecoder
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer 
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.TaskContext
import contextpack._


object ClickstreamConsumerStreaming {

  def consumerKafka(args: Array[String]) {

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"//"hdfs://namenode/sql/metadata/hive"

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    //val Array(brokers, topics) = args
    val topic = Set(args(0))
    val brokers = args(1)
    val sparkConf = new SparkConf()
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("spark.sql.catalogImplementation","hive")
      .setMaster("local[*]")
      .setAppName("p3")
    val sc = new SparkContext(sparkConf)
    val ssc  = new StreamingContext(sc, Seconds(2))

    // Create direct Kafka stream with brokers and topics
    //val topicsSet = topics.split(",").toSet
    
     val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "trojan_horse",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
     )

    //topics has to be Array type, not Strings
    //val topics = Set(topic)
    //val ssc = MainContext.getStreamingContext()
    val topicdstream = KafkaUtils.createDirectStream[String, String](
      // StreamingContext below, get current running StreamingContext imported from context package
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    
    
    
    
    // val warehouseLocation = "file:${system:user.dir}/spark-warehouse"//"hdfs://namenode/sql/metadata/hive"
    val ssql = SparkSession
      .builder
      .config(sparkConf)
      //.config("spark.master", "local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      //.config("spark.sql.catalogImplementation","hive")
      .enableHiveSupport()
      .getOrCreate()
   
    // Drop the table if it already exists 
    ssql.sql("DROP TABLE IF EXISTS mainhive")
    println("after deletion")
    // Create the table to store your streams 
    ssql.sql("CREATE TABLE mainhive(order_id STRING, customer_id STRING, product_id STRING, product_name STRING, product_category STRING, payment_type STRING, qty STRING, price STRING, datetime STRING, country STRING, city STRING, ecommerce_webname STRING, payment_txn_id STRING, payment_txn_success STRING, failure_reason STRING) STORED AS TEXTFILE")
    //ssql.sql("CREATE TABLE mainhive(order STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")

    println("after create table")

    topicdstream.foreachRDD {rdd => 
      rdd.foreach { record =>
        import ssql.implicits._
        //.value() returns deserialized value column
        val sc = SparkContext.getOrCreate()
        val value = record.value()
        
        val time = record.timestamp()

        //parallelize value into rdd
        val messagedf = sc.parallelize(List(value)).toDF()
    // Creates a temporary view using the DataFrame
    messagedf.createOrReplaceTempView("csmessages")
      
      //Insert continuous streams into Hive table
      ssql.sql("INSERT INTO TABLE mainhive SELECT * FROM csmessages")

      // Select the parsed messages from the table using SQL and print it (since it runs on drive display few records)
      val messagesqueryDF =
      ssql.sql("SELECT * FROM csmessages")
      println(s"========= $time =========")
      messagesqueryDF.show()
    }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
/** Case class for converting RDD to DataFrame */
//case class Record(recordtime: String,eventid: String,url: String,ip: String)
// val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

// val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//   ssc, kafkaParams, topicsSet)

// val lines = messages.map(_._2)
