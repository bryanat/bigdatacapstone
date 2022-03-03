package producerpack

import org.apache.spark.sql.SparkSession
import java.util.Random

object trend1 {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

  val trendTag = "TR1"

  //Set up the data that we will be using
  var locationVector = DataCollection.getCityCountryList(spark)
  var productVector = DataCollection.getProductDataList(spark)
  var customerVector = DataCollection.getCustomersList(spark)
  var failureVector = DataCollection.getfailReasonsList(spark)
  var websiteVector = DataCollection.getWebsiteList(spark)


  def main(args: Array[String]): Unit ={
    val random = new Random()
    val test = DataCollection.getBooksList(spark)
    val test2 = DataCollection.getSportsList(spark)
    test.foreach(println)
    test2.foreach(println)
  }
}
