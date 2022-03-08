package producerpack

import org.apache.spark.sql.SparkSession
import producerpack.DataCollection


object ExponentialIncreaseOverTimeTrend {
  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //we will used Camerons DataCollection object to get our data.
  def createTrend(dataPoints:Int):Vector[String] = {
    //I will try and
    var resultVector = Vector[String]()
    //get the data from object
    val collector = new DataCollection
    val customerData = collector.getCustomersList(spark)
    val productData = collector.getComputersList(spark)
    val failReasons = collector.getfailReasonsList(spark)
    val websites = collector.getWebsiteList(spark)
    val paymentTypes = collector.getPaymentList(spark)
    //This is the variance between the number of purchases a day, so they will be +/-10 purchances a day
    val purchaseAmountVariance = 10
    //base number of sales made per day
    val basePurchasesPerDay = 30
    //amount the price varies per purchase as a percentage
    val priceVariance = 0.05
    //finally, the step amount, which will be calculated. the max amount to change would be 10 times the start.
    val stepAmount:Double =  10/dataPoints
    //i think i can finally get that

    resultVector
  }
  def generateChangeCoefficient(ex:Double,baseInt:Int): Double ={
    //it will take
    ex*Math.pow(2,baseInt)
  }
}
