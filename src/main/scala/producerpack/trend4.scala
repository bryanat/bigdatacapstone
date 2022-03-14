package producerpack

import org.apache.spark.sql.SparkSession

import java.util.Random
import scala.collection.mutable.ListBuffer

// Trend Four will show a larger amount of online grocery orders from North America than any other country.
// Our return string will be in the following format:
/* 0- Order ID
/  1 - Customer ID
/  2 - Customer Name
/  3 - Product ID
/  4 - Product Name
/  5 - Product Category
/  6 - Payment Type
/  7 - QTY
/  8 - Price
/  9 - Datetime
/  10 - Country
/  11 - City
/  12 - Website
/  13 - Transaction ID
/  14 - Transaction Success
/  15 - Transaction fail reason
 */

object trend4 {

  val trendTag = "TRB"
  val rs = new RandomSelections
  val trans = new Transactions
  val random = new Random()


  def manipulateDate(inputTransaction: String, date: String): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""
    resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
      splitT(7) + "," + splitT(8) + ","  + date + "," + splitT(10) + "," + splitT(11) + "," + splitT(12) + ","+ splitT(13) + "," + splitT(14) + "," + splitT(15)
    return resultString
  }

  def manipulateDateAndPrice(inputTransaction: String, date: String): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""
    val price = BigDecimal(splitT(8).toDouble*getPercentOff()).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble.toString
    resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
      splitT(7) + "," + price + "," + date + "," + splitT(10) + "," + splitT(11) + "," + splitT(12) + ","+ splitT(13) + "," + splitT(14) + "," + splitT(15)
    return resultString
  }

  def getPercentOff(): Double ={
    val number = random.nextInt(4)
    number match {
      case 0 => .9
      case 1 => .8
      case 2 => .7
      case 3 => .5
    }
  }

  def getRandomYear(): String ={
    val result = random.nextInt(25)+2000
    result.toString
  }

  //This is the main driver of Trend1 that will return a vector of transaction strings.
  // For this trend I only want to look at grocery orders, so I createInitalTransactions using only 'Grocery'
  // the counter is integrated to ensure that I will have enough data entry points for Crypto/US to show a clear trend.
  def getTrend4(spark: SparkSession, returnAmount: Int): Vector[String]={
    var orderCounter = 100000
    var orderID = trendTag+orderCounter.toString
    var tempString = ""
    var dateStr = ""
    var resultList = ListBuffer("")

    // November 24th (Thursday) - Not On sale
    for (i <- 0 to returnAmount){
      tempString = trans.createInitialTransaction(rs, spark, orderID,"All")
      dateStr = getRandomYear()+"-11-24"
      tempString = manipulateDate(tempString, dateStr)
      resultList += tempString
      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString
    }
    //November 25th - still on sale
    for (i <- 0 to returnAmount*3){
      tempString = trans.createInitialTransaction(rs, spark, orderID,"All")
      dateStr = getRandomYear()+"-11-25"
      tempString = manipulateDateAndPrice(tempString, dateStr)
      resultList += tempString
      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString
    }
    //November 26th - Still on sale
    for (i <- 0 to returnAmount*3){
      tempString = trans.createInitialTransaction(rs, spark, orderID,"All")
      dateStr = getRandomYear()+"-11-26"
      tempString = manipulateDateAndPrice(tempString, dateStr)
      resultList += tempString
      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString
    }
    //November 27th - Not on sale
    for (i <- 0 to returnAmount){
      tempString = trans.createInitialTransaction(rs, spark, orderID,"All")
      dateStr = getRandomYear()+"-11-27"
      tempString = manipulateDate(tempString, dateStr)
      resultList += tempString
      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString
    }
    val resultVector = resultList.toVector
    resultVector
  }
}
