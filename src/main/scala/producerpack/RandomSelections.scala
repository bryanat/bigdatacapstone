package producerpack

import org.apache.spark.sql.SparkSession

import java.util.Random

class RandomSelections {

  val rs = new Random()
  val dc = new DataCollection

  def getRandomCustomerID(spark: SparkSession): String ={
    val customerList = dc.getCustomersList(spark)
    val randomNum = rs.nextInt(customerList.length-1)
    val randomResult = customerList(randomNum)
    val randomID = randomResult(0).toString
    val randomName = randomResult(1).toString
    val resultString = randomID+ "," + randomName + ","
    resultString
  }
  // getRandomWebsite
  def getRandomWebsite(spark: SparkSession): String ={
    val websiteList = dc.getWebsiteList(spark)
    val randomNum = rs.nextInt(websiteList.length-1)
    val randomResult = websiteList(randomNum)
    val randomURL = randomResult(1).toString
    val resultString = randomURL + ","
    resultString
  }
  // getRandomProduct
  def getRandomProduct(spark: SparkSession): String ={
    val productList = dc.getProductDataList(spark)
    val randomNum = rs.nextInt(productList.length-1)
    val randomResult = productList(randomNum)
    val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
    resultString
  }
  // getRandomCategory
  def getRandomCategory(spark: SparkSession): String ={
    val productList = dc.getProductDataList(spark)
    val randomNum = rs.nextInt(productList.length-1)
    val randomResult = productList(randomNum)
    val resultString = randomResult(2).toString
    resultString
  }
}
