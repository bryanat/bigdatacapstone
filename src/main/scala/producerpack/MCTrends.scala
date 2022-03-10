package producerpack

import org.apache.spark.sql.SparkSession

import java.util.Random
import java.util.Date
import scala.collection.mutable.ListBuffer

object MCTrends {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val trendTag = "ZOR"
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
  val dec17 = "12-17-2021"
  val dec18 = "12-18-2021"
  val dec19 = "12-18-2021"

  //getSpecificTypeFromCategory --currently not working-- would go in datacollection otherwise
  /*def getSpecificItemTypeFromCategory(spark: SparkSession): Vector[Row] = {
    val productVector = getProductDataList(spark)
    val productListBuffer = ListBuffer[Row]()
    productVector.foreach(println)
    val pattern = """A\w*""".r


    for (i <- 0 until productVector.length-1){
      if (productVector(i).get(2).toString == "Books" && (productVector(i).get(1).toString).matches("""A\w*"""))
      {
        productListBuffer += productVector(i)
      }
    }
    val productList = productListBuffer.toList
    val resultVector = productList.toVector
    resultVector
  }*/

  def createTransactionMCTrend(orderID: String, category: String, date: String): String={
    val initialString = orderID+ "," + rs.getRandomCustomerID(spark)+rs.getRandomProduct(spark, category)+rs.getRandomPayment(spark)+random.nextInt(25)+","+
      date+","+rs.getRandomLocation(spark)+rs.getRandomWebsite(spark)+ random.nextInt(204202) + "," +"success"
    initialString
  }

  def main(args: Array[String]): Unit = {
    var orderCounter = 100000
    var orderID = trendTag + orderCounter.toString
    for (i <- 0 to 10) {
      val resultString = createTransactionMCTrend(orderID, "Electronics",dec17)
      val resultString2 = createTransactionMCTrend(orderID, "Clothes",dec17)
      val resultString3 = createTransactionMCTrend(orderID, "Electronics",dec17)
      orderCounter = orderCounter + 1
      orderID = trendTag + orderCounter.toString
      println(resultString)
    }

  }
}
