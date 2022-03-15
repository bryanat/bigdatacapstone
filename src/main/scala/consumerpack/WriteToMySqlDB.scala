package consumerpack
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, SQLException}

import consumerpack._

object WriteToMySqlDB {
val warehouseLocation = "${system:user.dir}/spark-warehouse"
System.setProperty("hadoop.home.dir", "C:\\winutils")

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

      ssql.sparkContext.setLogLevel("ERROR")

// Drop the table if it already exists 
    ssql.sql("DROP TABLE IF EXISTS hivetable")
    // Create the table to store your streams 
    ssql.sql("CREATE TABLE hivetable (order_id STRING, customer_id STRING, customer_name STRING, product_id STRING, product_name STRING, " +
      "product_category STRING, payment_type STRING, qty STRING, price STRING, datetime STRING, country STRING, city STRING, " +
      "ecommerce_website_name STRING, payment_txn_id STRING, payment_txn_success STRING, failure_reason STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")

    // val df_main = ssql.read.option("multiline","true").parquet("dataset-offline/")

    val dfTest = ssql.sql("LOAD DATA LOCAL INPATH 'dataset-online/sample-of-final-data.csv' OVERWRITE INTO TABLE hivetable")
    
    val df_main = ssql.sql("SELECT * FROM hivetable")

    // val url = "jdbc:mysql://localhost:3306/p3"

    // val prop = new Properties()
    // prop.put("user","root")
    // prop.put("password","RiffRaff68#$")
    // Class.forName("com.mysql.jdbc.Driver")

    // ssql.table("hivetable").write.jdbc("jdbc:mysql://localhost:3306/p3", "hivetable", prop)

}
