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
  def createTrend(dataPoints:Int, pAmntVar:Int, baseAmount:Int, priceVar:Double):Vector[String] = {
    //I will try and
    var resultVector = Vector[String]()
    //get the data from object
    val collector = new DataCollection
    val customerData = collector.getCustomersList(spark)
    val productData = collector.getComputersList(spark)
    val failReasons = collector.getfailReasonsList(spark)
    val websites = collector.getWebsiteList(spark)
    val paymentTypes = collector.getPaymentList(spark)
    val stepAmount:Double =  10/dataPoints
    //i think i can finally get to doing the actual thing
    for(i<-0 until dataPoints) {
        //we will loop through until we have
    }

    resultVector
  }
  def generateChangeCoefficient(ex:Double,baseInt:Int): Double ={
    //it will take
    ex*Math.pow(2,baseInt)
  }
}
