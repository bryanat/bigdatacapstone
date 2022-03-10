package producerpack

import org.apache.spark.sql.SparkSession
import producerpack.DataCollection
import scala.util.Random

import scala.collection.mutable.ListBuffer


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
    val productData = collector.getCategoryList(spark, "Computers")
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

  def increaseDate(date:ListBuffer[Int]):ListBuffer[Int]={
    //date will come in MM, DD, YYYY
    //check specail months, 2 and 12. leap years arent considered
    date(0) match {
      case 2 =>
        if(date(1) == 28) {
          //roll month over, set day to one
          date(0)+=1
          date(1) = 1
          return date
        }
      case 12 =>
        if (date(1) == 31) {
          //roll month to 1, day to 1, year by 1
          date(0) = 1
          date(1) = 1
          date(2) += 1
          return date
        }
      case _ =>
    }
    //now that specail months are out of the way...
    if(date(0) < 7){
      println("In the first 7 months")
      date(0)%2 match {
        case 0 =>{
          if (date(1) == 30) {
            //roll month and set day to 1
            date(0) += 1
            date(1) = 1
            return date
          }
      }
        case 1 =>
          if (date(1) == 31){
            //roll month
            date(0) += 1
            date(1) = 1
            return date
          }
      }
    } else {
      date(0)%2 match {
        case 0 =>
          if(date(1) == 31) {
            //roll month and set day to 1
            date(0) += 1
            date(1) = 1
            return date
          }
        case 1 =>
          if (date(1) == 30){
            //roll month
            date(0) += 1
            date(1) = 1
            return date
          }
      }
    }
    date(1) += 1
    date
  }
}
