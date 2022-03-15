package consumerpack

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization._
import org.apache.spark.SparkConf

import java.util.Properties
import java.sql.{Connection, DriverManager}
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
      .enableHiveSupport()
      .getOrCreate()

/*
    val table = spark.read.option("header", "true").csv("dataset-online/samplecsv.csv")
    //table.show();
    table.createOrReplaceTempView("t1")
    spark.sql("create table if not exists t2 as select * from t1");
*/
    //-----

    val warehouseLocation = "${system:user.dir}/spark-warehouse"

    val sparkConf = new SparkConf()
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("spark.sql.catalogImplementation","hive")
      .setMaster("local[*]")
      .setAppName("p3")

    val ssql = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.executor.memory", "48120M")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    // Drop the table if it already exists
    ssql.sql("DROP TABLE IF EXISTS hivetable")
    // Create the table to store your streams
    ssql.sql("CREATE TABLE hivetable (order_id STRING, customer_id STRING, customer_name STRING, product_id STRING, product_name STRING, " +
      "product_category STRING, payment_type STRING, qty STRING, price STRING, datetime STRING, country STRING, city STRING, " +
      "ecommerce_website_name STRING, payment_txn_id STRING, payment_txn_success STRING, failure_reason STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")

    // val df_main = ssql.read.option("multiline","true").parquet("dataset-offline/")

    ssql.sql("LOAD DATA LOCAL INPATH 'dataset-online/sample-of-final-data.csv' OVERWRITE INTO TABLE hivetable")

    val table2 = ssql.sql("SELECT * FROM hivetable")

    //Thread.sleep(60000*10);



    //adding mysql connectivity - IT WORKS!!!! Just gotta make sure you're using jdk 1.8
    //creates a new table within db selected, with table titled as "hivetable"
    val prop=new Properties()
    prop.put("user","root")
    prop.put("password","dhayal")
    Class.forName("com.mysql.jdbc.Driver")

    val reading = ssql.read.jdbc("jdbc:mysql://localhost:3306/", "proj0.accounts", prop)
    reading.show()

    ssql.table("hivetable").write.jdbc("jdbc:mysql://localhost:3306/proj3", "hivetable", prop)



    /*
    //failed transactions
    val ft = spark.sql("select order_id, payment_txn_id, product_name, price, product_category, city, country from t1 where payment_txn_success = 'N'")
    ft.show()

    //failed transactions by location
    val ftLoc = spark.sql("select order_id, payment_txn_id, product_name, price, city, country from t1 where payment_txn_success = 'N' order by city desc, country desc")
    ftLoc.show()

    //failed transactions by price
    val ftPrice = spark.sql("select order_id, payment_txn_id, product_name, price from t1 where payment_txn_success = 'N' order by price desc")
    ftPrice.show()

    //failed transactions by category
    val ftCat = spark.sql("select order_id, payment_txn_id, product_name, product_category from t1 where payment_txn_success = 'N' order by product_category desc")
    ftCat.show()

    //count of each failure reason -- CANT DO ON SAMPLE DATA WITHOUT COL failure_reason
    //val frCount = spark.sql("select failure_reason, count(payment_txn_id) as quantity from t1 where payment_txt_success = 'N' group by failure_reason")
    //frCount.show()

    //count of successful transactions vs failure transactions
    val sVf = spark.sql("select payment_txn_success, count(payment_txn_id) as quantity from t1 group by payment_txn_success")
    sVf.show()
    */

}
