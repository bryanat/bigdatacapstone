import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrameWriter

import consumerpack._

object TestFile extends App {
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Spark Works Y'all")
  spark.sparkContext.setLogLevel("ERROR")

    println("hello kafka")
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    println("Main app started")
    MainConsumer.startMainConsumer(spark)
    //MainConsumer.Query1(spark)
  mandeepConsumer.popularProduct(spark)
  mandeepConsumer.popularMonth(spark)



}
