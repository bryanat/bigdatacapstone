package producerpack

import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql.{Row, SparkSession}
import producerpack.MainProducer.spark

import java.util.Calendar
import java.util.GregorianCalendar
import java.util.Random
import scala.collection.mutable.ListBuffer

class Transactions {


  val random = new Random()
  val dc = new DataCollection
  val gc = new GregorianCalendar()

  val productList = dc.getProductDataList(spark)
  val customerList = dc.getCustomersList(spark)
  val paymentList = dc.getPaymentList(spark)
  val locationList = dc.getCityCountryList(spark)
  val websiteList = dc.getWebsiteList(spark)
  val failList = dc.getfailReasonsList(spark)

  // The Base Transaction String that we will be manipulating in Trends
  def createInitialTransaction(rs:RandomSelections, spark: SparkSession, orderID: String, category: String): String={
    val failOrNo = getRandomSuccess()
    val initialString = orderID+ "," + rs.getRandomCustomerID(customerList, spark)+rs.getRandomProduct(productList, spark, category)+rs.getRandomPayment(paymentList, spark)+random.nextInt(25)+","+
      getRandomDate() + "," +rs.getRandomLocation(locationList, spark)+rs.getRandomWebsite(websiteList, spark)+ random.nextInt(204202) + "," + failOrNo + "," + rs.getRandomFail(failList, spark, failOrNo)
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

  def getRandomDate(): String={
    val year = randBetween(2000,2025)
    gc.set(Calendar.YEAR, year)
    val dayOfYear = randBetween(1, gc.getActualMaximum(Calendar.DAY_OF_YEAR))
    gc.set(Calendar.DAY_OF_YEAR, dayOfYear)
    val resultString = (gc.get(Calendar.YEAR)) + "-" + (gc.get(Calendar.MONTH) + 1) + "-" + gc.get(Calendar.DAY_OF_MONTH) + " "+ random.nextInt(23).toString +
      ":"+ getRandomMinutes() + ":" + getRandomMinutes()
    resultString
  }

  def getRandomTime(): String={
    random.nextInt(23).toString + ":"+ getRandomMinutes() + ":" + getRandomMinutes()
  }

  def getRandomSuccess(): String ={
    val ranNum = random.nextInt(5)
    if (ranNum == 1) {
      return "N"
    }
    "Y"
  }
  def randBetween(start: Int, end: Int): Int = {
    return start+Math.round(Math.random()*(end-start)).asInstanceOf[Int]
  }

  def getRandomMinutes(): String={
    val temp = random.nextInt(59)
    var tempString = temp.toString
    if (temp < 10){
      tempString = "0" + temp.toString
    }
    tempString
  }
}
