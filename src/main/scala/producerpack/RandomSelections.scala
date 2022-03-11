package producerpack

import org.apache.spark.sql.{Row, SparkSession}

import java.util.Random

// I created this file in hopes to make the construction of our transactions easier and reduce code redundancy.
// Please let me know if there are any other random selections you may find helpful, or if you don't think this is neccessary.

class RandomSelections {

  val rs = new Random()

  //getRandomCustomerID: Will return a string of both CustomerID&CustomerName
  def getRandomCustomerID(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(0).toString + "," + randomResult(1).toString + ","
    resultString
  }
  // getRandomWebsite: Will return a string of only a random domain (URL)
  def getRandomWebsite(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(1).toString + ","
    resultString
  }
  def getRandomPayment(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(0).toString + ","
    resultString
  }

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