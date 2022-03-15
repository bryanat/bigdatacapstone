package consumerpack

import com.github.mrpowers.spark.daria.sql.DariaWriters
import contextpack.MainContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import java.io.File
import scala.reflect.io.Directory
import scala.io.StdIn._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Hdfs, Path}
import org.apache.spark.SparkConf

class RYConsumer {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  private var spark = SparkSession.builder()
    .appName("ConsumerQuery")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  val successYes = "Y"
  val successNo = "N"
  //val outputPath = new Path("webhdfs://44.195.89.83:9000")
  val outputPath = new Path("data/output")
  var waitForMinute = 10
  val neededMilliseconds = 60000
  val hdfsAddress = "webhdfs://ec2-44-195-89-83.compute-1.amazonaws.com:9000"
  val fileType = ".csv"

  var sparkConf = new SparkConf()
  var table = spark.read.option("delimiter", ",").option("header", "true").csv("dataset-online/sample-of-final-data.csv")
  var tableName = " "
  var fileFormat, csvLocation = " "

  def setConsumerSpark(sparkSession: SparkSession, tableName: String): Unit =
  {
    spark = sparkSession
    this.tableName = tableName
    queryConsumer
  }



  def queryConsumer():Unit = {
    table = spark.sql(s"select * from $tableName")
    table.createTempView("t1")
//    5. change in price/cost ("sale") for products by timeframe

    spark.sql("drop table if exists t2")
    spark.sql("create table if not exists t2 as select * from t1")
    val productTrend = spark.sql("select product_name, price as sale, DateTime from t1 group by product_name, price, DateTime order by DateTime asc")
    saveToCSV(productTrend, MainContext.getSparkSession(), "productTrendDF")
    //productTrend.write.option("header", "true").mode("overwrite").csv(s"$hdfsAddress/"+"v1/example/productTrend")
    saveToCSV(productTrend, spark, "productTrendDF")
//    6. popular cities/countries by most purchases

    val cityDF = spark.sql(s"select city, count(payment_txn_id) as purchase_count from t1 where payment_txn_success = '$successYes' group by city order by purchase_count desc")
    //cityDF.show()
    val countryDF = spark.sql(s"select country, count(payment_txn_id) as purchase_count from t1 where payment_txn_success = '$successYes' group by country order by purchase_count desc")
    //countryDF.show()
    saveToCSV(cityDF, MainContext.getSparkSession(), "cityDF")
    saveToCSV(countryDF, MainContext.getSparkSession(), "countryDF")
    //WaitForSeconds(waitForMinute)

  }

  def saveToCSV(df: DataFrame, sparkSession: SparkSession, fileName: String): Unit = {
    fileFormat = fileName +fileType
    println("Creating Excel file...")
    DariaWriters.writeSingleFile(df = df, format = "csv", sc = sparkSession.sparkContext,
      tmpFolder = outputPath.toString(), filename = outputPath+"\\"+fileFormat)
    csvLocation = outputPath+"\\"+fileFormat
  }

  //only use this when in testing where livestreaming is NOT possible otherwise this will halt entire showcase
  //using this will allow power bi - visualization tool - to access spark-warehouse and retrieve necessary tables
  def WaitForSeconds(timeLimit: Int): Unit = {
    Thread.sleep(neededMilliseconds * timeLimit)
  }
}
