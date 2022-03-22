package consumerpack
import org.apache.spark._
import org.apache.spark.sql._
import java.util.Properties
import java.sql.{Connection, DriverManager}

object writingToMySqlDB extends App{
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  //def main(args: Array[String]): Unit = {
  val warehouseLocation = "${system:user.dir}/spark-warehouse"
  val sparkConf = new SparkConf()
    .set("spark.sql.warehouse.dir", warehouseLocation)
    .set("spark.sql.catalogImplementation", "hive")
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

  /*val dfTest = ssql.sql("LOAD DATA LOCAL INPATH 'input/sample-of-final-data.csv' OVERWRITE INTO TABLE hivetable")*/
  val dfTest = ssql.sql("LOAD DATA LOCAL INPATH 'input/data-snapshot.txt' OVERWRITE INTO TABLE hivetable")
  ssql.sql("SELECT * FROM hivetable").show()

  //create a connection with MYSql
  val url = "jdbc:mysql://localhost:3306/p3"
  val prop = new Properties()
  prop.put("user","root")
  prop.put("password","matwal")
  Class.forName("com.mysql.jdbc.Driver")
  // var connection:Connection = DriverManager.getConnection(url, username, password)

  //create a table in MYSQL
  println("popular product categories,average price per product category")
  import org.apache.spark.sql.SaveMode
  ssql.table("hivetable").write.jdbc(url,"hivetable", prop)
  val sqlDf3 = ssql.sql("select product_category,MAX(product_count),AVG_price from " +
    "(SELECT extract(MONTH from datetime)as month,extract(YEAR from datetime)as year,product_category," +
    "COUNT(product_category) as product_count,round(AVG(price),2) as AVG_price from " +
    "hivetable WHERE payment_txn_success = 'Y' GROUP BY month,year,product_category )"+
    "group by product_category,AVG_price order by MAX(product_count) DESC ")
  sqlDf3.show(300)
  sqlDf3
    .coalesce(1) // number of parts/files
    .write
    .mode(SaveMode.Append)
    .option("header", true)
   // .csv("output/Q1csv")

  println("Most Popular Dates For Purchases")


 /* val sqlDf4=ssql.sql("SELECT product_name,datetime,Count(*) from hivetable WHERE payment_txn_success = 'Y' " +
    "GROUP BY datetime,product_name ORDER BY Count(*) DESC LIMIT 10")
  sqlDf4.show(300)*/

  val sqlDf4=ssql.sql("select cast(datetime as date) as dates, " +
    "count(order_id) as c from hivetable where payment_txn_success = 'Y' " +
    "group by dates order by c desc limit 10")
  sqlDf4.show(300)
  /*val sqlDf4 = ssql.sql("SELECT extract(DAY from datetime)as day,extract(MONTH from datetime)as month," +
    "extract(YEAR from datetime)as year,Count(*) from hivetable WHERE payment_txn_success = 'Y' " +
    "GROUP BY day,month,year ORDER BY Count(*) DESC LIMIT 10")*/
 /* sqlDf4.show(300)
  sqlDf4
    .coalesce(1) // number of parts/files
    .write
    .mode(SaveMode.Append)
    .option("header", true)*/
    //.csv("output/Q2csv")
}
