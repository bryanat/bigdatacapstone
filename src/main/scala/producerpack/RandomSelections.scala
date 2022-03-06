package producerpack

import org.apache.spark.sql.SparkSession

import java.util.Random

// I created this file in hopes to make the construction of our transactions easier and reduce code redundancy.
// Please let me know if there are any other random selections you may find helpful, or if you don't think this is neccessary.

class RandomSelections {

  val rs = new Random()
  val dc = new DataCollection

  //getRandomCustomerID: Will return a string of both CustomerID&CustomerName
  def getRandomCustomerID(spark: SparkSession): String ={
    val customerList = dc.getCustomersList(spark)
    val randomNum = rs.nextInt(customerList.length-1)
    val randomResult = customerList(randomNum)
    val randomID = randomResult(0).toString
    val randomName = randomResult(1).toString
    val resultString = randomID+ "," + randomName + ","
    resultString
  }
  // getRandomWebsite: Will return a string of only a random domain (URL)
  def getRandomWebsite(spark: SparkSession): String ={
    val websiteList = dc.getWebsiteList(spark)
    val randomNum = rs.nextInt(websiteList.length-1)
    val randomResult = websiteList(randomNum)
    val randomURL = randomResult(1).toString
    val resultString = randomURL + ","
    resultString
  }
  // getRandomProduct: Will return a string of a random product selected from ProductData
  def getRandomProduct(spark: SparkSession): String ={
    val productList = dc.getProductDataList(spark)
    val randomNum = rs.nextInt(productList.length-1)
    val randomResult = productList(randomNum)
    val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
    resultString
  }
  // getRandomCategory: Will return a string of a random Category
  def getRandomCategory(spark: SparkSession): String ={
    val productList = dc.getProductDataList(spark)
    val randomNum = rs.nextInt(productList.length-1)
    val randomResult = productList(randomNum)
    val resultString = randomResult(2).toString
    resultString
  }
}
