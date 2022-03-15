package consumerpack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import consumerpack._


object JacobQuery extends App {

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

    // Need to declare df variable
    val df_electronics = df_main.where(df_main("product_category") === "Electronics") //Alternative query
    // val df_electronics = df_main.where("product_category == Electronics")

//     df_main.show()

// df_electronics.show()

//     query to find the count of computer related purchases over time from earliest to year 2022
//     Will need regex to pull yearly numbers

// filters out Computers-related purchases and then orders these purchases by datetime
val df_Computer = (
  df_main
  .where(df_main("product_category") === "Computers")
  // .where(df_main("product_category") === "Computers" && df_main("datetime") === "2001-11-26" ) //Alternative query
  .orderBy(asc("datetime"))
).persist()

// df_Computer.show()

// finds the count of all payment types from 2000-2024
lazy val df_ComputerPayTypeCount = df_Computer
  .select("payment_type")
  .groupBy("payment_type").count()

// df_ComputerPayTypeCount.show()

// write queries output out to CSV file
// write.mode(Append) - loop for new data to write to CSV every 30 seconds


df_ComputerPayTypeCount.repartition(1).write.format("csv").mode("append").saveAsTable("ComputerPaymentTypes")

// query to find popular computer purchase by year

lazy val dfPopPurchComputer = df_Computer
  .select("product_name")
  .groupBy("product_name").count()
  .orderBy(count("product_name"))

// dfPopPurchComputer.orderBy(desc("count")).show()

// popularity of Hostess Snowballs over time (annual) - possible unionall select query

// lazy val dfSnowball = df_main
//   .select("")

// query to find the count of payment_type crypto over time where country equals United States



// query to find maximum instances in ecommmerce websites in United States
// query to find maximum instances in ecommmerce websites in United States

}
