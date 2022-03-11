package producerpack

import org.apache.spark.sql.SparkSession

import java.util.Random
import scala.collection.mutable.ListBuffer

// Trend Eight will show a larger amount of items over $750 sold at expensiveitems.com
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

object trend8 {

  val trendTag = "TR8"
  val rs = new RandomSelections
  val trans = new Transactions
  val random = new Random()

  //here we will inject our randomly created string with information that we need to create a trend
  //in this case, every third transaction will be updated to contain 'Crypto' as payment type and 'United States' as the country
  // We receive a comma separated string, split it by ",", use the array to create a new string, and return.

  def manipulateTransactionTrend8(inputTransaction: String): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""
    val qty = ((random.nextInt(4))+1).toString
    resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
      qty + "," + splitT(8) + "," + splitT(9) + "," + splitT(10) + "," + splitT(11) + "," + "expensiveitems.com" + ","+ splitT(13) + "," + splitT(14) + "," + splitT(15)
    resultString
  }

  //This is the main driver of Trend1 that will return a vector of transaction strings.
  // For this trend I only want to look at grocery orders, so I createInitalTransactions using only 'Grocery'
  // the counter is integrated to ensure that I will have enough data entry points for Crypto/US to show a clear trend.
  def getTrend8(spark: SparkSession, returnAmount: Int): Vector[String]={
    var orderCounter = 100000
    var orderID = trendTag+orderCounter.toString
    var resultList = ListBuffer("")
    for (i <- 0 to returnAmount) {
      val tempString = trans.createInitialTransaction(rs, spark, orderID,"highEnd")
      val resultString = manipulateTransactionTrend8(tempString)
      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString
      resultList += resultString
    }
    val resultVector = resultList.toVector
    resultVector
  }
}

