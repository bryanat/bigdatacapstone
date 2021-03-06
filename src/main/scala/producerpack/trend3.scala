package producerpack

import org.apache.spark.sql.SparkSession

import java.util.Random
import scala.collection.mutable.ListBuffer

// Trend Three will show a large amount of Electronic Orders in the United States between 10 p.m. and 2 a.m.
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

object trend3 {

  val trendTag = "TR3"
  val rs = new RandomSelections
  val trans = new Transactions
  val random = new Random()

  //here we will inject our randomly created string with information that we need to create a trend
  //in this case, we will inject a large amount of data between the period 10 p.m. -> 2 a.m. where electronics have the most sales.
  // We receive a comma separated string, split it by ",", use the array to create a new string, and return.
  def manipulateTransactionTrend3(inputTransaction: String): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""
      resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
        (splitT(7).toInt*3).toString + "," + splitT(8) + "," + trans.getRandomDate() + " " + getTimeBetween() + "," + "United States" + "," + splitT(11) + "," + splitT(12) + ","+ splitT(13) + "," + splitT(14) + "," + splitT(15)
    resultString
  }

  def getTimeBetween(): String ={
//    random.nextInt(4).toString + ":"+ trans.getRandomMinutes() + ":" + trans.()
      var hours = ""
      val randomNum = random.nextInt(4)
        randomNum match {
          case 0 => hours = "22"
          case 1 => hours = "23"
          case 2 => hours = "00"
          case 3 => hours = "01"
        }
    val resultStr = hours+":"+trans.getRandomMinutes()+":"+trans.getRandomMinutes()
    resultStr
  }

  def getTrend3(spark: SparkSession, returnAmount: Int): Vector[String]={
    var orderCounter = 100000
    var orderID = trendTag+orderCounter.toString
    var resultList = ListBuffer("")
    for (i <- 0 to returnAmount) {
      val tempString = trans.createInitialTransaction(rs, spark, orderID,"Electronics")
      val resultString = manipulateTransactionTrend3(tempString)
      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString
      resultList += resultString
    }
    val resultVector = resultList.toVector
    resultVector
  }
}

