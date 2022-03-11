package consumerpack

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import java.time.Duration
import scala.collection.JavaConverters._

class RYConsumer {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("ConsumerQuery")
    .config("spark.master", "local")
    .getOrCreate()


  val table = spark.read.option("delimiter", ",").option("header", "true").csv("dataset-online/testfile1.csv")

  val successYes = "Y"
  val successNo = "N"

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
    table.createTempView("t1")
//    5. change in price/cost ("sale") for products by timeframe

    val table2 = table.select(col("datetime"), to_date(col("DateTime"), "MM/dd/yyyy").as("DateTime"))
    table2.createTempView("t2")
    //spark.sql("select t1.product_name, t1.price as sale, t2.DateTime from t1 left join t2 using (datetime) order by t2.DateTime asc").show()

//    6. popular cities/countries by most purchases

    spark.sql(s"select city, count(payment_txn_id) as purchase_count from t1 where payment_txn_success = '$successYes' group by city order by purchase_count desc").show()
    spark.sql(s"select country, count(payment_txn_id) as purchase_count from t1 where payment_txn_success = '$successYes' group by country order by purchase_count desc").show()
  }
}
