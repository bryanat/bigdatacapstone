package producerpack

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

// Trend Two will show downtime in Visa payments for a two day period.
// Our return string will be in the following format:
// "order_id,customer_id,customer_name,product_id,product_name,product_category,payment_type,qty,price,datetime,country,city,website,pay_id,success"

object trend2 {

  val trendTag = "TR2"
  val rs = new RandomSelections
  val trans = new Transactions

  //here we will inject our randomly created string with information that we need to create a trend
  //in this case, I will be manipulating the DATE, Payment, Success, and FailReason fields of our string
  // This will happen ON date 10-15-2008
  // We receive a comma separated string, split it by ",", use the array to create a new string, and return.
  def manipulateTransactionTrend2(inputTransaction: String): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""

    resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
      "Visa" + "," + "2008-10-15" + " " + trans.getRandomTime() + "," + splitT(9) + "," + splitT(10) + "," + splitT(11) + "," + splitT(12) + ","+ splitT(13) + "," + "N" + "," + "103"
    resultString
  }

  def manipulateTransactionTrend2BeforeDate(inputTransaction: String): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""
    resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
      "Visa" + "," + "2008-10-14" + " " + trans.getRandomTime() + "," + splitT(9) + "," + splitT(10) + "," + splitT(11) + "," + splitT(12) + ","+ splitT(13) + "," + splitT(14) + "," + splitT(15)
    return resultString
  }

  def manipulateTransactionTrend2AfterDate(inputTransaction: String): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""
    resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
      "Visa" + "," + splitT(8) + "," + "2008-10-16" + " " + trans.getRandomTime() + "," + splitT(10) + "," + splitT(11) + "," + splitT(12) + "," + splitT(13) + "," + splitT(14) + "," + splitT(15)
    return resultString
  }

  //This is the main driver of Trend1 that will return a vector of transaction strings.
  // For this trend I only want to look at grocery orders, so I createInitalTransactions using only 'Grocery'
  // the counter is integrated to ensure that I will have enough data entry points for Crypto/US to show a clear trend.
  def getTrend2(spark: SparkSession, returnAmount: Int): Vector[String]={
    var orderCounter = 100000
    var orderID = trendTag+orderCounter.toString
    var repeatCounter = 1
    var tempStr = ""
    var resultStr = ""
    var resultList = ListBuffer("")
    for (i <- 0 to returnAmount) {
      tempStr = trans.createInitialTransaction(rs, spark, orderID, "All")
      resultStr = manipulateTransactionTrend2BeforeDate(tempStr)
      resultList += resultStr
      orderCounter = orderCounter + 1
      orderID = trendTag+orderCounter.toString
    }
    for (i <- 0 to returnAmount*2){
      tempStr = trans.createInitialTransaction(rs, spark, orderID, "All")
      resultStr = manipulateTransactionTrend2(tempStr)
      resultList += resultStr
      orderCounter = orderCounter + 1
      orderID = trendTag+orderCounter.toString
    }

    for (i <- 0 to returnAmount) {
      tempStr = trans.createInitialTransaction(rs, spark, orderID, "All")
      resultStr = manipulateTransactionTrend2AfterDate(tempStr)
      resultList += resultStr
      orderCounter = orderCounter + 1
      orderID = trendTag+orderCounter.toString
    }

    val resultVector = resultList.toVector
    resultVector
  }
}

