package kafkapack

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.streaming._
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer 
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.TaskContext
import contextpack._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}



object ClickstreamConsumerStreaming {

  def consumerKafka(args: Array[String]): Unit = {

    //////if data does not load into hive table, delete spark-warehouse and metastore_db and try again///////////////////
    val topic = Set(args(0))
    val brokers = args(1)
    val warehouseLocation = args(2)
    val sparkConf = new SparkConf()
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("spark.sql.catalogImplementation","hive")
      .setMaster("local[*]")
      .setAppName("p3")
      //.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
    val sc = new SparkContext(sparkConf)
    val ssc  = new StreamingContext(sc, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")


    //set kafka consumer config
     val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "trojan_horse",
    //earliest will throw an error if no message streams in when consumer starts
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
    //  "heartbeat.interval.ms" -> "3000",
    //  "session.timeout.ms" -> "10000"
    //"isolation.level"-> "read.committed"
    )

    //subscribe to kafka: topics are Array/Set type, not Strings
    val topics = Set(topic)
    val topicdstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )
    
    //configure spark session
    val ssql = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.executor.memory", "48120M") 
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

      // val hadoopConf = sc.hadoopConfiguration
      // hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
      
      // Drop the main table if it already exists 
    ssql.sql("DROP TABLE IF EXISTS hivetable1")
      // Create the main table to store your streams 
    ssql.sql("CREATE TABLE IF NOT EXISTS hivetable1(order_id STRING, customer_id STRING," +
        " customer_name STRING, product_id STRING, product_name STRING, product_category STRING, " +
        "payment_type STRING, qty STRING, price STRING, datetime STRING, country STRING, city STRING, " +
        "ecommerce_website_name STRING, payment_txn_id STRING, payment_txn_success STRING, failure_reason STRING, timestamp STRING) " +
        //" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','STORED AS TEXTFILE LOCATION 'hdfs://44.200.236.7:9000/user/hive/warehouse/'")
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','STORED AS TEXTFILE")

    ssql.sql("DROP TABLE IF EXISTS baddata1")
    ssql.sql("CREATE TABLE IF NOT EXISTS baddata1(order_id STRING, timestamp STRING) " +
      //"STORED AS TEXTFILE LOCATION 'hdfs://44.200.236.7:9000/user/hive/warehouse/'")
      "STORED AS TEXTFILE")
    val now = System.currentTimeMillis() 
    println(s"(Consumer) Current unix time is: $now")
    
    //deserialize the messages from kafka
    val results = topicdstream.map(record=>Tuple2(record.key(), record.value()))
    //gets the value column
    val lines = results.map(_._2)
    
    
    
    //creates the schema for dataframes
    val schemaString = "order_id,customer_id,customer_name,product_id,product_name,product_category,payment_type,qty," +
      "price,datetime,country,city," +
    "ecommerce_website_name,payment_txn_id,payment_txn_success,failure_reason"
    val schema = StructType(schemaString.split(",", -1).map(fieldName => StructField(fieldName, StringType, true)))
    val schemabadString = "order_id"
    val schemabad = StructType(schemabadString.split(",", -1).map(fieldName=>StructField(fieldName, StringType, true)))

    lines.foreachRDD {rdd =>
        //when rdds are streamed in...
        if (rdd!=null) {
          //try {
            val ssql = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
            import ssql.implicits._
            //udf for adding the timestamp column
            val currenttimeudf = udf(()=>System.currentTimeMillis())
            //samples from rdd for testing purposes
            //val samplerdd = rdd.sample(false, 0.05, 123)
            val rowRDD = rdd.map(_.split(","))
            val goodRDD = rowRDD.filter(row=>row.length==16) 
            //records with extra or missing columns will be filtered out 
            val badRDD= rowRDD.filter(row=>row.length!=16).map(e=>Row(e(0)))
            val baddf = ssql.createDataFrame(badRDD, schemabad).withColumn("timestamp", currenttimeudf())
            //order IDs of bad data is inserted into bad data table
            baddf.write
            //.option("path", "hdfs://44.200.236.7:9000/user/hive/warehouse/")
            .mode("append").insertInto("baddata1") 
            ssql.sql("SELECT * FROM baddata1").show()
            //good data is inserted into the main hivetable
            val finalRDD = goodRDD.map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
            val df = ssql.createDataFrame(finalRDD, schema).withColumn("timestamp", currenttimeudf())
            df.write
            //.option("path", "hdfs://44.200.236.7:9000/user/hive/warehouse/")
            .mode("append").insertInto("hivetable1")
            ssql.sql("SELECT * FROM hivetable1").show()
          
      // }
      //   catch {
      //     case e: NumberFormatException=>println("bad data in quantity or price") 
      //   }
        }
    }
      ssc.start()
      ssc.awaitTermination()

  }
}












