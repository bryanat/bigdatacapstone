package producerpack

import contextpack.MainContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

object DataCollection {

//  System.setProperty("hadoop.home.dir", "c:/winutils")
//  val spark = SparkSession
//    .builder()
//    .appName("project1")
//    .config("spark.master","local")
//    .enableHiveSupport()
//    .getOrCreate()
//  spark.sparkContext.setLogLevel("ERROR")

  def getProductDataList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/productdata.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

  def getCityCountryList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/citylist.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }
  def getWebsiteList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/websites(500).csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }
  def getfailReasonsList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/failReasons.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }
  def getCustomersList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/custIdNames.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

}
