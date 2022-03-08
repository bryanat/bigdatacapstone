package producerpack

import org.apache.spark.sql.{Row, SparkSession}
import producerpack.trend1.spark

import java.util.Random
import scala.collection.mutable.ListBuffer

class Transactions {


  val random = new Random()
  val dc = new DataCollection

  val productList = dc.getProductDataList(spark)
  val customerList = dc.getCustomersList(spark)
  val paymentList = dc.getPaymentList(spark)
  val locationList = dc.getCityCountryList(spark)
  val websiteList = dc.getWebsiteList(spark)

  def createInitialTransaction(rs:RandomSelections, spark: SparkSession, orderID: String, category: String): String={
    val initialString = orderID+ "," + rs.getRandomCustomerID(customerList, spark)+rs.getRandomProduct(productList, spark, category)+rs.getRandomPayment(paymentList, spark)+random.nextInt(25)+","+
      "10-02-2017,"+rs.getRandomLocation(locationList, spark)+rs.getRandomWebsite(websiteList, spark)+ random.nextInt(204202) + "," +"success"
    initialString
  }

  // Returns Random 100 Transactions with no applied Trend
  def getRandomTransactions(rs: RandomSelections, spark: SparkSession, returnAmount: Int): Vector[String]={
    var orderCounter = 100000
    var orderID = "NOT"+orderCounter.toString
    var repeatCounter = 1
    var resultList = ListBuffer("")
    var tempString = ""
    for (i <- 0 to returnAmount) {
      tempString = createInitialTransaction(rs, spark, orderID,"All")
      orderCounter = orderCounter+1
      orderID = "NOT"+orderCounter.toString
      resultList += tempString
    }
    println()
    val resultVector = resultList.toVector
    resultVector
  }
}
