package producerpack

import org.apache.spark.sql.SparkSession
import java.util.Random

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


  def main(args: Array[String]): Unit = {
    val test = dc.getBooksList(spark)
    val test2 = dc.getSportsList(spark)
//    test.foreach(println)
//    test2.foreach(println)
//    val test3 = dc.filterByPriceAbove(spark, 500)
//    test3.foreach(println)
//    val test4 = dc.filterByPriceBelow(spark, 500)
//    test4.foreach(println)
    println(rs.getRandomCustomerID(spark))
    println(rs.getRandomCustomerID(spark))
    println(rs.getRandomCustomerID(spark))
    println(rs.getRandomWebsite(spark))
    println(rs.getRandomWebsite(spark))
    println(rs.getRandomWebsite(spark))
    println(rs.getRandomProduct(spark))
    println(rs.getRandomProduct(spark))
    println(rs.getRandomProduct(spark))
    println(rs.getRandomCategory(spark))
    println(rs.getRandomCategory(spark))
    println(rs.getRandomCategory(spark))

//    println(customerVector(49))
//    println(customerVector(49).get(1))
  }
}

