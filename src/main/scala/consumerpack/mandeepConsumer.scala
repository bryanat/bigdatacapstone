package consumerpack

/*import kafka.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{IntegerDeserializer,StringDeserializer}
import java.time.Duration*/

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.Duration
import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}
//for date
import org.apache.spark.sql.functions.{to_date, to_timestamp}

object mandeepConsumer
{
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("ConsumerQuery")
    .config("spark.master", "local")
    .getOrCreate()

    println("Main Consumer started...")
    import spark.implicits._
    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header", true).option("inferSchema", true).format("csv").load(
      "input/productFile.csv").toDF("order_id", "customer_id", "customer_name", "product_id", "product_name", "product_category",
      "payment_type", "price","datetime","country","city","ecommerce_website_name","payment_txn_id","payment_txn_success")
    //Changing data type of Obs_Date column to "DateType"
    //val modifiedDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))
    val modifiedDF = dfTest.withColumn("datetime", to_date($"datetime", "MM/dd/yyyy mm:ss").cast("timestamp"))
    //Creating temporary view "ProductFile" from modifiedDF
    modifiedDF.createOrReplaceTempView("Product")


  /*def consumerTest(): Unit = {

    println("Inside Consumer")

    val props = new Properties()
    props.put("bootstrap.servers", "SERVER DETAILS HERE")
    props.put("group.id", "CONSUMER GROUP NAME")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])
    val kafkaConsumer = new KafkaConsumer[String, String](props)
    kafkaConsumer.subscribe(TOPICS_NAME)
    while (true) {
      val result = kafkaConsumer.poll(2000).asScala
      for ((topic, data) <- result) {
        //OPERATIONS THAT WILL OCCUR WHEN STREAMING DATA
      }
    }
  }*/
  def consumerTest(): Unit = {

    println("Inside Consumer")
    val consumerProperties = new Properties()

    consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-1")
    consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

    val consumer = new KafkaConsumer[Int, String](consumerProperties)
    consumer.subscribe(List("test_2").asJava)

    println("| Key | Message | Partition | Offset |")
    while (true) {
      val polledRecords: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(1))
      if (!polledRecords.isEmpty) {
        println(s"Polled ${polledRecords.count()} records")
        val recordIterator = polledRecords.iterator()
        while (recordIterator.hasNext) {
          val record: ConsumerRecord[Int, String] = recordIterator.next()
          val csvTrip = record.value()
          println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
        }
      }
      println(polledRecords.count())
    }
  }

  /*def Query1(spark: SparkSession): Unit = {
    println("get the total values from table")
    val sqlDf3 = spark.sql("select *from Product")
    sqlDf3.show(300)
  }*/
  def popularProduct(spark:SparkSession): Unit = {
    println("Display popular product categories and average price per product categories group by product_category ")
    val sqlDf3 = spark.sql("select month,year,product_category,MAX(product_count),AVG_price from " +
      "(SELECT extract(MONTH from datetime)as month,extract(YEAR from datetime)as year,product_category," +
      "COUNT(product_category) as product_count,round(AVG(price),2) as AVG_price from " +
      "Product WHERE payment_txn_success = 'Y' GROUP BY month,year,product_category " +
      "ORDER BY month,product_count DESC) group by month,year,product_category,AVG_price")
    sqlDf3.show(300)
  }

  def popularMonth(spark:SparkSession): Unit = {
    println("most popular dates for purchases")
    val sqlDf4 = spark.sql("SELECT extract(DAY from datetime)as day,extract(MONTH from datetime)as month," +
      "extract(YEAR from datetime)as year,Count(*) from Product WHERE payment_txn_success = 'Y' " +
      "GROUP BY day,month,year ORDER BY Count(*) DESC LIMIT 10")
    sqlDf4.show(300)
  }
}
