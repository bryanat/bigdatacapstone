package producerpack

import org.apache.spark.sql.SparkSession
import shapeless.syntax.std.tuple.productTupleOps

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
  val dc = new DataCollection

  //Set up the data that we will be using
  var locationVector = dc.getCityCountryList(spark)
  var customerVector = dc.getCustomersList(spark)
  var failureVector = dc.getfailReasonsList(spark)
  var websiteVector = dc.getWebsiteList(spark)
  var electronicVector = dc.getElectronicsList(spark)
  val random = new Random()

  var groceryVector = dc.getGroceryList(spark)


//Monday - string to array to lIST to original
  //goal: create a new string
  //REPLACE ELEMENT WITH LIST BUFFER
  //PULL FROM LIST
  //SAVE TO VARIABLE
  //APPLY MATH ON THAT VARIABLE
  def createInitialTransaction(orderID: String, category: String): String={
    val initialString = orderID+ "," + rs.getRandomCustomerID(spark)+rs.getRandomProduct(spark, category)+rs.getRandomPayment(spark)+random.nextInt(25)+","+
      "10-02-2017,"+rs.getRandomLocation(spark)+rs.getRandomWebsite(spark)+"pay_id,"+"success"
    val arr = initialString.split((","))
    val buf = new ListBuffer[String]()
    arr.foreach(elem => buf += elem)
    println("List Buffer : " + buf)
    println(s"Element at index 0 = ${buf(0)}")
    println(s"Element at index 1 = ${buf(1)}")
    println(s"Element at index 2 = ${buf(2)}")
    println(s"Element at index 3 = ${buf(3)}")
    println(s"Element at index 4 = ${buf(4)}")
    println(s"Element at index 5 = ${buf(5)}")
    println(s"Element at index 6 = ${buf(6)}")
    println(s"Element at index 7 = ${buf(7)}")
    println(s"Element at index 8 = ${buf(8)}")
    println(s"Element at index 9 = ${buf(9)}")
    println(s"Element at index 10 = ${buf(10)}")
    println(s"Element at index 11 = ${buf(11)}")
    println(s"Element at index 12 = ${buf(12)}")
    println(s"Element at index 13 = ${buf(13)}")
    println(s"Element val tempNum = buf(6)")
    //TUESDAY
    var tempNum = buf(8)
    val result = tempNum*2
    buf.update(8,result.toString)
println(buf(8))
 //   initialString
//
    //var test2 = new ListBuffer[String]()
 //   var abc = initialString.split(",")
    //noew abc is an array
   // println(abc)
    //var test = abc(abc.length-1).replace("success","Y")
    //println("array with one element replaced")
   // var testStr = test.toString
    //println("this should be the new string"+testStr)
    //add elements to list buffer
   // test += initialString
    //convert ListBuffer to a list by using .toList
   // return testStr
  }


  def main(args: Array[String]): Unit = {
//    var orderCounter = 100000
//    var orderID = trendTag+orderCounter.toString
//    for (i <- 0 to 10) {
//        println(createInitialTransaction(orderID,"All"))
//      orderCounter = orderCounter+1
//      orderID = trendTag+orderCounter.toString
//
//      }


    // ALL OF THE COMMENTED BELOW IS JUST FOR TESTING DIFFERENT METHODS OF DataCollection AND RandomSelections
//    val test = dc.getGroceryList(spark)
//    val test2 = dc.getSportsList(spark)
//    test.foreach(println)
//    test2.foreach(println)
//    val test3 = dc.filterByPriceAbove(spark, 1000.00)
//    test3.foreach(println)
//    println()
    val test4 = dc.filterByPriceBelow(spark, 500)
    test4.foreach(println)
println(dc.getMaxPrice(spark))
    println(createInitialTransaction("10000","All"))

    //    val test4 = dc.filterByPriceBelow(spark, 500)
//    test4.foreach(println)
//    println(rs.getRandomCustomerID(spark))
//    println(rs.getRandomCustomerID(spark))
//    println(rs.getRandomCustomerID(spark))
//    println(rs.getRandomWebsite(spark))
//    println(rs.getRandomWebsite(spark))
//    println(rs.getRandomWebsite(spark))
//    println(rs.getRandomProduct(spark))
//    println(rs.getRandomProduct(spark))
//    println(rs.getRandomProduct(spark))
//    println(rs.getRandomCategory(spark))
//    println(rs.getRandomCategory(spark))
//    println(rs.getRandomCategory(spark))

//    println(customerVector(49))
//    println(customerVector(49).get(1))
  }
}

