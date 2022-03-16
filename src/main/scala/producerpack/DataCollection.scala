package producerpack

import contextpack.MainContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class DataCollection {

  // These first get Methods are created to easily import our data from CSV files into vectors. This process involves
  // CSV --> DataFrame --> List --> Scala List --> Vector

  //getProductDataList will return the following Rows in a Vector: [product_id, product_name, product_category, price]
  def getProductDataList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/productdata.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

  //getCityCountryList will return the following Rows in a Vector: [city, country]
  def getCityCountryList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/citylist.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

  //getWebsiteList will return the following Rows in a Vector: [rank, root domain, linking root domains, domain authority]
  def getWebsiteList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/websites(500).csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

  //getfailReasonsList will return the following Rows in a Vector: [failCode]

  def getfailReasonsList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/failReasons.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

  //getCustomersList will return the following Rows in a Vector: [customer_id, customer_name]
  def getCustomersList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/custIdNames.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

  def getPaymentList(spark: SparkSession): Vector[Row] ={
    val df = spark.read.csv("dataset-online/paymentTypes.csv")
    val productList = df.collectAsList()
    val productListScala = productList.asScala.toList
    val returnVector = productListScala.toVector
    returnVector
  }

  def getCategoryList(spark:SparkSession, category: String): Vector[Row] ={

    val productVector = getProductDataList(spark)
    val productListBuffer = ListBuffer[Row]()
    for (i <- 0 until productVector.length-1){
      if (productVector(i).get(2).toString == category){
        productListBuffer += productVector(i)
      }
    }
    val productList = productListBuffer.toList
    val resultVector = productList.toVector
    resultVector
  }

  //We will collect all of our prices into a list, convert the list into a list of doubles, apply the max function, and then return the result.
  def getMaxPrice(spark: SparkSession): Double = {
    val df = spark.read.csv("dataset-online/productdata.csv")
    val productVector = getProductDataList(spark)
    val listOfPrices = ListBuffer("1.0")
    var tempPrice = ""

    for (i <- 0 until productVector.length - 1) {
      tempPrice = productVector(i).get(3).asInstanceOf[String]
      listOfPrices += tempPrice
    }
    listOfPrices.remove(1)
    var listOfPricesDoubles = listOfPrices.map(x => x.toDouble)
    val maxPrice = listOfPricesDoubles.max
    maxPrice
  }

  // The main idea behind the filtering system is to keep a record of all index's that contain a price above a certain amount.
  // After we've collected the indexes, we will pull ONLY those indexes and add them to a new list, which will then be turned into a vector.
  // This method relies that the order of our products will stay static (which they will be, I'm assuming)
  def filterByPriceAbove(spark: SparkSession, filter: Double): Vector[Row] ={
    val productVector = getProductDataList(spark)
    val listOfPrices = ListBuffer("1.0")
    var tempPrice = ""
    var indexKeeper = ListBuffer[Int]()
    var resultList = ListBuffer[Row]()

    for (i <- 0 until productVector.length - 1) {
      tempPrice = productVector(i).get(3).asInstanceOf[String]
      listOfPrices += tempPrice
    }

    for (i <- 1 to productVector.length - 2) {
      if (listOfPrices(i).toDouble > filter){
        indexKeeper += i-1
      }
      indexKeeper.foreach(x => resultList += productVector(x))
    }
    val resultVector = resultList.toVector
    resultVector
  }

  // Same process as filterByPriceAbove, however switched the comparison.
  def filterByPriceBelow(spark: SparkSession, filter: Double): Vector[Row] ={
    val productVector = getProductDataList(spark)
    val listOfPrices = ListBuffer("1.0")
    var tempPrice = ""

    for (i <- 0 until productVector.length - 1) {
      tempPrice = productVector(i).get(3).asInstanceOf[String]
      listOfPrices += tempPrice
    }

    var indexKeeper = ListBuffer[Int]()
    var resultList = ListBuffer[Row]()

    for (i <- 1 to productVector.length - 2) {
      if (listOfPrices(i).toDouble < filter){
        indexKeeper += i-1
      }
      indexKeeper.foreach(x => resultList += productVector(x))
    }
    val resultVector = resultList.toVector
    resultVector
  }

}
