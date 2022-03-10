package consumerpack

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization._
import java.util.Properties
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

/*
object YashConsumer {
    def main(args: Array[String]) = {

        val props = new Properties()
        props.put("bootstrap.servers", "SERVER DETAILS HERE")
        props.put("group.id", "CONSUMER GROUP NAME")
        props.put("key.deserializer", classOf[StringDeserializer])
        props.put("value.deserializer", classOf[StringDeserializer])

        val kafkaConsumer = new KafkaConsumer[String, String](props)
        kafkaConsumer.subscribe(COLLECTION_OF_TOPICS)

        while (true) {
            val result = kafkaConsumer.poll(2000).asScala
            for ((topic, data) <- result) {
                //OPERATIONS THAT WILL OCCUR WHEN STREAMING DATA
            }
        }
    }
}
 */

object YashConsumer extends App {
  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .getOrCreate()

  val table =
    spark.read.option("header", "true").csv("dataset-online/samplecsv.csv")
  // table.show();
  table.createOrReplaceTempView("t1")

  // failed transactions
  spark
    .sql(
      "select order_id, payment_txn_id, product_name, price, product_category, city, country from t1 where payment_txn_success = 'N'"
    )
    .show()

  // failed transactions by location
  spark
    .sql(
      "select order_id, payment_txn_id, product_name, price, city, country from t1 where payment_txn_success = 'N' order by city desc, country desc"
    )
    .show()

  // failed transactions by price
  spark.sql(
    "select order_id, payment_txn_id, product_name, price from t1 where payment_txn_success = 'N' order by price desc"
  )

  // failed transactions by category
  spark
    .sql(
      "select order_id, payment_txn_id, product_name, product_category from t1 where payment_txn_success = 'N' order by product_category desc"
    )
    .show()

  // count of each failure reason -- CANT DO ON SAMPLE DATA WITHOUT COL failure_reason
  // spark.sql("select failure_reason, count(payment_txn_id) as quantity from t1 where payment_txt_success = 'N' group by failure_reason").show()

  // count of successful transactions vs failure transactions
  spark
    .sql(
      "select payment_txn_success, count(payment_txn_id) as quantity from t1 group by payment_txn_success"
    )
    .show()
}
