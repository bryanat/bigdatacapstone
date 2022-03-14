package consumerpack

import org.apache.spark._
import org.apache.spark.sql._

object QueryStatic {

  def main(args: Array[String]): Unit = {

  val warehouseLocation = "file:///D:/Revature/Project3/bigdatacapstone/spark-warehouse"
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
    
    ssql.sql("SELECT * FROM hivetable").show()

    //Find number of each payment type and order by descending
    // **Possible Use: We can find the number 1 used payment method and see if it was used by a much larger amount**
    ssql.sql("SELECT payment_type as Payment_Type, COUNT(*) as Total_Payments FROM hivetable GROUP BY payment_type ORDER BY Total_Payments DESC").show()

    // Find the Success and Failures of each payment type
    // **Possible Use: If we see a large number of 'N' for a category we know that it probably had a day of failures**
    ssql.sql("SELECT payment_type, payment_txn_success, COUNT(*) as Total FROM hivetable GROUP BY payment_type, payment_txn_success ORDER BY payment_type, payment_txn_success DESC").show(100)

    // Find the Minimum and Maximum paid price for a product.
    // **Possible Use: We can see if the price changes, and how often it did change. This will allow us to see if items went on sale or if prices trended upwards**
    ssql.sql("SELECT product_name, count(*) as num_prices, min(price) as first_price, max(price) as last_price FROM hivetable GROUP BY product_name").show()

    // Find the Total Sales of each category by Country
    // **Possible Use: We can see if a specific country had a larger number of sales in a category compared to other countries**
    ssql.sql("SELECT product_category, country, COUNT(*) as Total_Sales, row_number() over (partition by product_category order by COUNT(*) DESC) as Category_Rank FROM hivetable GROUP BY product_category, country HAVING COUNT(*)>5 ORDER BY product_category ASC, Total_Sales DESC").show(100)

    //Top 3 Countries in Sales per Category
    // **Possible Use: We can see if a specific country had a larger number of sales in a category compared to other countries**
    ssql.sql("SELECT * from (SELECT product_category, country, COUNT(*) as Total_Sales, row_number() over (partition by product_category order by COUNT(*) DESC) as Category_Rank FROM hivetable GROUP BY product_category, country HAVING COUNT(*)>5 ORDER BY product_category ASC, Total_Sales DESC) ranks WHERE Category_Rank <= 3").show(100)

    //Total Quantity sold per item
    ssql.sql("SELECT product_name, SUM(qty) as Total_Sold FROM hivetable GROUP BY product_name ORDER BY Total_Sold DESC").show(100)
  }
}