package kafkapack
//import _root_.kafka.serializer.StringDecoder
<<<<<<< HEAD
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
=======
import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
>>>>>>> refs/remotes/origin/kafka/bryan
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer 
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.TaskContext
<<<<<<< HEAD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

import contextpack._


=======
>>>>>>> refs/remotes/origin/kafka/bryan

object ClickstreamConsumerStreaming {

  def consumerKafka(args: Array[String]) {

<<<<<<< HEAD
    val warehouseLocation = "/home/bryanat/bigdatacapstone/dstream1"//"hdfs://namenode/sql/metadata/hive"

    //System.setProperty("hadoop.home.dir", "C:\\hadoop")
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
    ssc.sparkContext.setLogLevel("ERROR")
=======
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
>>>>>>> refs/remotes/origin/kafka/bryan

    // Create direct Kafka stream with brokers and topics
    //val topicsSet = topics.split(",").toSet
    
     val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
<<<<<<< HEAD
    "group.id" -> "trojan_horse",
=======
    "group.id" -> "use_a_separate_group_id_for_each_stream",
>>>>>>> refs/remotes/origin/kafka/bryan
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
     )

    //topics has to be Array type, not Strings
<<<<<<< HEAD
    val topics = Set(topic)
=======
    //val topics = Set(topic)
>>>>>>> refs/remotes/origin/kafka/bryan
    //val ssc = MainContext.getStreamingContext()
    val topicdstream = KafkaUtils.createDirectStream[String, String](
      // StreamingContext below, get current running StreamingContext imported from context package
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    
    
    
    
<<<<<<< HEAD
    val ssql = SparkSession
=======
    
    
    
    
    
    // val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)



    
    // val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //   ssc, kafkaParams, topicsSet)

    // val lines = messages.map(_._2)
    
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val spark = SparkSession
>>>>>>> refs/remotes/origin/kafka/bryan
      .builder
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
   
    // Drop the table if it already exists 
<<<<<<< HEAD
    ssql.sql("DROP TABLE IF EXISTS kafkahivetable")
    // Create the table to store your streams 
    ssql.sql("CREATE TABLE kafkahivetable(random STRING, order_id STRING, customer_id STRING, product_id STRING, " +
      "product_name STRING, product_category STRING, price STRING, payment_type STRING, qty STRING, " +
      "datetime STRING, city STRING, country STRING, ecommerce_webname STRING, payment_txn_id STRING, " +
      "payment_txn_success STRING) STORED AS TEXTFILE")

    println("after create table")

    val now = System.currentTimeMillis()
    println(s"(Consumer) Current unix time is: $now")
    topicdstream.foreachRDD {rdd => 
      
      //val ssql = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      
      val schema = StructType( Array(
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true),
                 StructField("language", StringType,true)
             ))

      val arrayOfCollectedRDDs = rdd.collect()

      arrayOfCollectedRDDs.foreach { record =>



        val ssql2 = SparkSession
        .builder
        .config(sparkConf)
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()
        ssql2.newSession()

        // val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
        


        
        //get new spark session 
        //val sc = SparkContext.getOrCreate()

        val value = record.value() 
        val time = record.timestamp()
        val v = value.split(",")


        //import ssql2.implicits._
        //get new spark context
        //val sc = SparkContext.getOrCreate()
        //.value() returns deserialized value column
        
  
        
        //try {
          //RDD[String] to RDD[Case Class] to DF
        // val messagedf = sc.parallelize(List(value.split(",")))
        // .map(x=>Transaction(x(0).toString, x(1).toString, x(2).toString, x(3).toString,
        //  x(4).toString,x(5).toString, x(6).toString, x(7).toString, x(8).toString, x(9).toString,
        // x(10).toString,x(11).toString, x(12).toString, x(13).toString, x(14).toString))
        // .toDF()
      // Creates a temporary view using the DataFrame
      // messagedf.show()
      // messagedf.createOrReplaceTempView("csmessages")
      val v1 = v(0)
      val v2 = v(1)
      val v3 = v(2)
      val v4 = v(3)
      val v5 = v(4)
      val v6 = v(5)
      val v7 = v(6)
      val v8 = v(7)
      val v9 = v(8)
      val v10 = v(9)
      val v11 = v(10)
      val v12 = v(11)
      val v13 = v(12)
      val v14 = v(13)
      val v15 = v(14)
      //Insert continuous streams into Hive table
      //ssql.sql("INSERT INTO TABLE kafkahivetable SELECT * FROM csmessages")

              
        val rowRDD = rdd.map(attributes => Row(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15))
        import ssql2.implicits._
        val df = ssql2.createDataFrame(rowRDD, schema)
        df.write.format("parquet").mode("append").save("dataset-online/wut")        

    //ssql.sql(s"INSERT INTO TABLE kafkahivetable VALUES ('$v1', '$v2', '$v3', '$v4','$v5','$v6','$v7','$v8','$v9','$v10','$v11','$v12','$v13','$v14','$v15')")

      // Select the parsed messages from the table using SQL and print it (since it runs on drive display few records)
    //val messagesqueryDF = ssql2.sql("SELECT * FROM kafkahivetable")
      println(s"========= $time =========")
    //messagesqueryDF.show()
    //} catch {case e: NullPointerException=>println("message not added to table")} 
  }
  }

=======
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
>>>>>>> refs/remotes/origin/kafka/bryan
    ssc.start()
    ssc.awaitTermination()

  }
}
/** Case class for converting RDD to DataFrame */
<<<<<<< HEAD
case class Transaction(order_id: String,customer_id: String,customer_name: String,product_id: 
  String,product_name: String,product_category: String,price: String,payment_type:String,qty:String,datetime:String,
   city:String, country:String, ecommerce_webname:String, payment_txn_id:String, payment_txn_success:String)
// val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

// val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//   ssc, kafkaParams, topicsSet)

// val lines = messages.map(_._2)
=======
case class Record(recordtime: String,eventid: String,url: String,ip: String)
>>>>>>> refs/remotes/origin/kafka/bryan
