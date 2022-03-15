package consumerpack

import org.apache.spark._
import org.apache.spark.sql._

import consumerpack._


object JacobQuery {

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

    ssql.sql("LOAD DATA LOCAL INPATH 'dataset-online/sample-of-final-data.csv' OVERWRITE INTO TABLE hivetable")
    
    val df_main = ssql.sql("SELECT * FROM hivetable")

    // Need to declare df variable
    // val df_electronics = df.main.where(df_main("product_category") == "Electronics")

    df_main.show()

//     query to find the count of computer related purchases over time from earliest to year 2022
//     Will need regex to pull yearly numbers

// query to find popular computer purchase by year
// regex

// popularity of Hostess Snowballs over time (annual)

// query to find the count of payment_type crypto over time where country equals United States

// query to find maximum instances in ecommmerce websites in United States
// query to find maximum instances in ecommmerce websites in United States

}
