package producerpack

import org.apache.spark.sql.SparkSession

import java.util.Random
import java.util.Date
import scala.collection.mutable.ListBuffer

object MCTrends {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val trendTag = "ZOR"
  val rs = new RandomSelections
  val dc = new DataCollection

  //Set up the data that we will be using
  var locationVector = dc.getCityCountryList(spark)
  var customerVector = dc.getCustomersList(spark)
  var failureVector = dc.getfailReasonsList(spark)
  var websiteVector = dc.getWebsiteList(spark)
  var electronicVector = dc.getElectronicsList(spark)
  val random = new Random()

  var groceryVector = dc.getGroceryList(spark)
  val dec17 = "12-17-2021"
  val dec18 = "12-18-2021"
  val dec19 = "12-19-2021"

  def createTransactionMCTrend(orderID: String, category: String, date: String): String={
    val initialString = orderID+ "," + rs.getRandomCustomerID(spark)+rs.getRandomProduct(spark, category)+rs.getRandomPayment(spark)+random.nextInt(25)+","+
      date+","+rs.getRandomLocation(spark)+rs.getRandomWebsite(spark)+ random.nextInt(204202) + "," +"success"
    initialString
  }

  def main(args: Array[String]): Unit = {
    var orderCounter = 100000
    var orderID = trendTag + orderCounter.toString
    for (i <- 0 to 10) {
      val resultString = createTransactionMCTrend(orderID, "Electronics",dec17)
      val resultString2 = createTransactionMCTrend(orderID, "Clothing",dec17)
      val resultString3 = createTransactionMCTrend(orderID, "Home & Garden",dec17)
      val resultString4 = createTransactionMCTrend(orderID, "Electronics",dec18)
      val resultString5 = createTransactionMCTrend(orderID, "Clothing",dec18)
      val resultString6 = createTransactionMCTrend(orderID, "Home & Garden",dec18)
      val resultString7 = createTransactionMCTrend(orderID, "Electronics",dec19)
      val resultString8 = createTransactionMCTrend(orderID, "Clothing",dec19)
      val resultString9 = createTransactionMCTrend(orderID, "Home & Garden",dec19)
      orderCounter = orderCounter + 1
      orderID = trendTag + orderCounter.toString
      println(resultString)
      println(resultString2)
      println(resultString3)
      println(resultString4)
      println(resultString5)
      println(resultString6)
      println(resultString7)
      println(resultString8)
      println(resultString9)
    }

  }
}
