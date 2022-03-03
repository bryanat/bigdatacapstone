package consumerpack

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.spark.sql._

import java.time.Duration
import scala.collection.JavaConverters._

class RYConsumer {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  val spark = SparkSession.builder()
    .appName("ConsumerQuery")
    .config("spark.master", "local")
    .getOrCreate()


  val df = spark.read.json("dataset-offline/yelp_academic_dataset_business.json")

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

  def queryConsumer():Unit = {

//    5. change in price/cost ("sale") for products by timeframe
//
//    -- with extract()
//    select price as sale, extract (year_month from datetime) as YEAR_MONTH from TABLE_NAME order by YEAR_MONTH asc
//
//    -- with REGEXP()
//    select price as sale, datetime as Year_Month from TABLE_NAME where datetime REGEXP '^[1-2][0-9][0-9][0-9]-[0-1][0-9]' order by YEAR_MONTH asc;

    df.show(30)

//    val dfQ5 = df2.groupBy("name","city", "review_count")
//      .agg(avg("stars").as("review_score")).sort(col("city"))
//      .sort(col("review_score").desc).sort(col("review_count").desc).where(col("review_score") >= 4.5) limit 10
//    dfQ5.show(10)

  }
}
