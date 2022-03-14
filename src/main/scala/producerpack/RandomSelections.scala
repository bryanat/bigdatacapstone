package producerpack

<<<<<<< HEAD
import org.apache.spark.sql.SparkSession
=======
import org.apache.spark.sql.{Row, SparkSession}
>>>>>>> refs/remotes/origin/kafka/bryan

import java.util.Random

// I created this file in hopes to make the construction of our transactions easier and reduce code redundancy.
// Please let me know if there are any other random selections you may find helpful, or if you don't think this is neccessary.

class RandomSelections {

  val rs = new Random()
<<<<<<< HEAD
  val dc = new DataCollection

  //getRandomCustomerID: Will return a string of both CustomerID&CustomerName
  def getRandomCustomerID(spark: SparkSession): String ={
    val customerList = dc.getCustomersList(spark)
    val randomNum = rs.nextInt(customerList.length-1)
    val randomResult = customerList(randomNum)
=======

  //getRandomCustomerID: Will return a string of both CustomerID&CustomerName
  def getRandomCustomerID(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
>>>>>>> refs/remotes/origin/kafka/bryan
    val resultString = randomResult(0).toString + "," + randomResult(1).toString + ","
    resultString
  }
  // getRandomWebsite: Will return a string of only a random domain (URL)
<<<<<<< HEAD
  def getRandomWebsite(spark: SparkSession): String ={
    val websiteList = dc.getWebsiteList(spark)
    val randomNum = rs.nextInt(websiteList.length-1)
    val randomResult = websiteList(randomNum)
    val resultString = randomResult(1).toString + ","
    resultString
  }
  def getRandomPayment(spark: SparkSession): String ={
    val paymentList = dc.getPaymentList(spark)
    val randomNum = rs.nextInt(paymentList.length-1)
    val randomResult = paymentList(randomNum)
=======
  def getRandomWebsite(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(1).toString + ","
    resultString
  }
  def getRandomPayment(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
>>>>>>> refs/remotes/origin/kafka/bryan
    val resultString = randomResult(0).toString + ","
    resultString
  }

<<<<<<< HEAD
  def getRandomLocation(spark: SparkSession): String ={
    val locationList = dc.getCityCountryList(spark)
    val randomNum = rs.nextInt(locationList.length-1)
    val randomResult = locationList(randomNum)
    val resultString = randomResult(0).toString + "," + randomResult(1).toString + ","
    resultString
  }
  // getRandomProduct: Will return a string of a random product selected from ProductData
  def getRandomProduct(spark: SparkSession, category: String): String ={

    category match {
      case "All" => {
        val productList = dc.getProductDataList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Computers" => {
        val productList = dc.getComputersList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Clothing" => {
        val productList = dc.getClothingList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Home & Garden" => {
        val productList = dc.getHomeGardenList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Grocery" => {
        val productList = dc.getGroceryList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Sports" => {
        val productList = dc.getSportsList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Automotive" => {
        val productList = dc.getAutomotiveList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Electronics" => {
        val productList = dc.getElectronicsList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Shoes" => {
        val productList = dc.getShoesList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Books" => {
        val productList = dc.getBooksList(spark)
        val randomNum = rs.nextInt(productList.length-1)
        val randomResult = productList(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
    }

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
=======
  def getRandomLocation(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(0).toString + "," + randomResult(1).toString + ","
    resultString
  }
  def getRandomFail(vector: Vector[Row], spark: SparkSession, checkFail: String): String ={
    if (checkFail.equals("Y")){
      return "100"
    }
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    var resultString = randomResult(0).toString
    if (resultString.equals("100")){
      resultString = "101"
    }
    resultString
  }
  // getRandomProduct: Will return a string of a random product selected from ProductData
  def getRandomProduct(vector: Vector[Row], spark: SparkSession): (String,String) ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + ","
    val price = randomResult(3).toString
    (resultString,price)
  }
  // getRandomCategory: Will return a string of a random Category
  def getRandomCategory(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(2).toString
    resultString
  }
}
>>>>>>> refs/remotes/origin/kafka/bryan
