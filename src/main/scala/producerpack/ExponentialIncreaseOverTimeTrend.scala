package producerpack

import org.apache.spark.sql.SparkSession
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
  def createTrend(dataPoints:Int, pAmntVar:Int, baseAmount:Int, startDate:ListBuffer[Int]):Vector[String] = {
    //I will try and
    val resultVector = ListBuffer[String]()
    val rs = new RandomSelections
    //get the data from object
    val collector = new DataCollection
    val customerData = collector.getCustomersList(spark)
    val productData = collector.getCategoryList(spark, "Computers")
    val failReasons = collector.getfailReasonsList(spark)
    val websites = collector.getWebsiteList(spark)
    val paymentTypes = collector.getPaymentList(spark)
    val places = collector.getCityCountryList(spark)
    var stepAmount:Double =  2/dataPoints.toDouble
    var curStep:Double = stepAmount
    val exCo =0.1
    //i think i can finally get to doing the actual thing
    var pntsDone = 0
    var orderID = 1736672
    var payID = 1823640
    var curDate = startDate
    while(pntsDone < dataPoints) {
      //amount of purchase for the day
      val varience = Random.nextInt(pAmntVar)
      val purchases = baseAmount + varience
      for(i<-0 to purchases) {
        if(pntsDone % 20 == 0){
          //introduce bad data roughly 5% of the time.
          //get a random customer
          val cus = rs.getRandomCustomerID(customerData,spark)
          val ogString = rs.getRandomProduct(productData,spark)
          val prod = ogString._1
          val fail = rs.getRandomFail(failReasons, spark, "N")
          val web = rs.getRandomWebsite(websites, spark)
          val pay = rs.getRandomPayment(paymentTypes,spark)
          val qty = Random.nextInt(10) +1.toString
          //hey rounding sucks in scala

          val price = BigDecimal(ogString._2.toDouble*generateChangeCoefficient(exCo,stepAmount)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble.toString()

          val country = rs.getRandomLocation(places,spark)

          resultVector += s"TGA${orderID.toString},$cus$prod$pay$qty,$price,${printDate(curDate)},$country$web${payID.toString},Y,$fail"
        } else {
          //introduce bad data roughly 5% of the time.
          //get a random customer
          val cus = rs.getRandomCustomerID(customerData,spark)
          val ogString = rs.getRandomProduct(productData,spark)
          val prod = ogString._1
          val fail = rs.getRandomFail(failReasons, spark, "N")
          val web = rs.getRandomWebsite(websites, spark)
          val pay = rs.getRandomPayment(paymentTypes,spark)
          val qty = Random.nextInt(10) +1.toString
          //hey rounding sucks in scala
          val testing:Double = ogString._2.toDouble*generateChangeCoefficient(exCo,curStep).asInstanceOf[Double]
          val t2 = generateChangeCoefficient(exCo,stepAmount).asInstanceOf[Double]
          val price = BigDecimal(testing).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble.toString

          val country = rs.getRandomLocation(places,spark)

          resultVector += s"TGA${orderID.toString},$cus$prod$pay$qty,$price,${printDate(curDate)},$country$web${payID.toString},Y,100"
        }
        orderID +=5
        payID += 1
        pntsDone += 1
        curStep += stepAmount
      }
      curDate = increaseDate(curDate)
    }
    //shuffle and return the final vector
    resultVector.toVector


  }
  def generateChangeCoefficient(ex:Double,baseStep:Double): Double ={
    //it will take
    ex*Math.pow(2,baseStep)
  }

  def increaseDate(date:ListBuffer[Int]):ListBuffer[Int]={
    //date will come in MM, DD, YYYY
    //check special months, 2 and 12. leap years arent considered
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
    //now that special months are out of the way...
    if(date(0) < 7){
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
  def printDate(date: ListBuffer[Int]):String = {
    date(0).toString + "-" + date(1).toString + "-" + date(2).toString
  }
}
