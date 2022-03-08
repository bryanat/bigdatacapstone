package producerpack

import org.apache.spark.sql.SparkSession
import java.util.Random
import java.util.Date
import scala.collection.mutable.ListBuffer

// Trend One will show a larger amount of online grocery orders from North America than any other country.
// Our return string will be in the following format:
// "order_id,customer_id,customer_name,product_id,product_name,product_category,payment_type,qty,price,datetime,country,city,website,pay_id,success"

object trend1 {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val trendTag = "TR1"
  val rs = new RandomSelections
  val trans = new Transactions

  //Set up the data that we will be using


  def manipulateTransactionTrend1(inputTransaction: String, counter: Int): String = {
    val splitT = inputTransaction.split(",")
    var resultString = ""
    if (counter == 3){
      resultString = splitT(0) + "," + splitT(1) + "," + splitT(2) + "," + splitT(3) + "," + splitT(4) + "," + splitT(5) + "," + splitT(6) + "," +
        "Crypto" + "," + splitT(8) + "," + splitT(9) + "," + splitT(10) + "," + "United States" + "," + splitT(12) + ","+ splitT(13) + "," + splitT(14)
      return resultString
    }
    resultString = inputTransaction
    resultString
  }

  def getTrend1(returnAmount: Int): Vector[String]={
    var orderCounter = 100000
    var orderID = trendTag+orderCounter.toString
    var repeatCounter = 1
    var resultList = ListBuffer("")
    for (i <- 0 to returnAmount) {
      val tempString = trans.createInitialTransaction(rs, spark, orderID,"Grocery")
      val resultString = manipulateTransactionTrend1(tempString, repeatCounter)
      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString
      repeatCounter = repeatCounter + 1
      resultList += resultString
      if(repeatCounter == 4){
        repeatCounter=1
      }
    }
    val resultVector = resultList.toVector
    resultVector
  }


//  def main(args: Array[String]): Unit = {
//
//
//
//
//
//
//
//
//
//
//
//
//
//    // ALL OF THE COMMENTED BELOW IS JUST FOR TESTING DIFFERENT METHODS OF DataCollection AND RandomSelections
////    val test = dc.getGroceryList(spark)
////    val test2 = dc.getSportsList(spark)
////    test.foreach(println)
////    test2.foreach(println)
////    val test3 = dc.filterByPriceAbove(spark, 1000.00)
////    test3.foreach(println)
////    println()
////    val test4 = dc.filterByPriceBelow(spark, 500)
////    test4.foreach(println)
////    val test4 = dc.filterByPriceBelow(spark, 500)
////    test4.foreach(println)
////    println(rs.getRandomCustomerID(spark))
////    println(rs.getRandomCustomerID(spark))
////    println(rs.getRandomCustomerID(spark))
////    println(rs.getRandomWebsite(spark))
////    println(rs.getRandomWebsite(spark))
////    println(rs.getRandomWebsite(spark))
////    println(rs.getRandomProduct(spark))
////    println(rs.getRandomProduct(spark))
////    println(rs.getRandomProduct(spark))
////    println(rs.getRandomCategory(spark))
////    println(rs.getRandomCategory(spark))
////    println(rs.getRandomCategory(spark))
//
////    println(customerVector(49))
////    println(customerVector(49).get(1))
//  }
}

