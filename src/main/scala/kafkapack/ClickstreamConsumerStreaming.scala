package kafkapack
//import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer 
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.TaskContext

object ClickstreamConsumerStreaming {

  def consumerKafka(args: Array[String]) {

    // if (args.length < 2) {
    //   System.err.println(s"""
    //     |Usage: ClickstreamSparkstreaming <brokers> <topics> 
    //     |  <brokers> is a list of one or more Kafka brokers
    //     |  <brokers> is a list of one or more Kafka topics to consume from
    //     |
    //     """.stripMargin)
    //   System.exit(1)
    // }


    //val Array(brokers, topics) = args
    val topic = Set(args(0))
    val brokers = args(1)
    val sparkConf = new SparkConf().setAppName("DirectKafkaClickstreams")
    // Create context with 10-second batch intervals
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct Kafka stream with brokers and topics
    //val topicsSet = topics.split(",").toSet
    
     val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
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
    
    
    
    
    
    
    
    
    
    // val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)



    
    // val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //   ssc, kafkaParams, topicsSet)

    // val lines = messages.map(_._2)
    
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
   
    // Drop the table if it already exists 
    spark.sql("DROP TABLE IF EXISTS kafka_to_hive_table")
    // Create the table to store your streams 
    spark.sql("CREATE TABLE kafka_to_hive_table (order_id STRING, customer_id STRING, customer_name STRING, product_id STRING, product_name STRING, " +
      "product_category STRING, payment_type STRING, qty STRING, price STRING, datetime STRING, country STRING, city STRING, " +
      "ecommerce_website_name STRING, payment_txn_id STRING, payment_txn_success STRING, failure_reason STRING, timestamp STRING) STORED AS TEXTFILE")
    // Convert RDDs of the lines DStream to DataFrame and run a SQL query






topicdstream.foreachRDD {rdd => 
      rdd.foreach { record =>
        import spark.implicits._
        //.value() returns deserialized value column
        val sc = SparkContext.getOrCreate()
        val value = record.value()
        val time = record.timestamp()
        //parallelize value into rdd
        val messagedf = sc.parallelize(List(value)).toDF()
    // Creates a temporary view using the DataFrame
      messagedf.createOrReplaceTempView("csmessages")
      
      //Insert continuous streams into Hive table
      spark.sql("INSERT INTO TABLE kafka_to_hive_table SELECT * FROM csmessages")

      // Select the parsed messages from the table using SQL and print it (since it runs on drive display few records)
      val messagesqueryDF =
      spark.sql("SELECT * FROM csmessages")
      println(s"========= $time =========")
      messagesqueryDF.show()
    }
    }


    // lines.foreachRDD { (rdd: RDD[String], time: Time) =>
      
    // import spark.implicits._
    //   // Convert RDD[String] to RDD[case class] to DataFrame
 
    //  val messagesDataFrame = rdd.map(_.split(",")).map(w => Record(w(0), w(1), w(2), w(3))).toDF()
      
    //   // Creates a temporary view using the DataFrame
    //   messagesDataFrame.createOrReplaceTempView("csmessages")
      
    //   //Insert continuous streams into Hive table
    //   spark.sql("INSERT INTO TABLE kafka_to_hive_table SELECT * FROM csmessages")

    //   // Select the parsed messages from the table using SQL and print it (since it runs on drive display few records)
    //   val messagesqueryDataFrame =
    //   spark.sql("SELECT * FROM csmessages")
    //   println(s"========= $time =========")
    //   messagesqueryDataFrame.show()
    // }
  // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
/** Case class for converting RDD to DataFrame */
case class Record(recordtime: String,eventid: String,url: String,ip: String)