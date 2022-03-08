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
  // getRandomProduct: Will return a string of a random product selected from ProductData
  def getRandomProduct(vector: Vector[Row], spark: SparkSession, category: String): String ={

    category match {
      case "All" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Computers" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Clothing" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Home & Garden" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Grocery" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Sports" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Automotive" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Electronics" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Shoes" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
      case "Books" => {
        val randomNum = rs.nextInt(vector.length-1)
        val randomResult = vector(randomNum)
        val resultString = randomResult(0).toString + "," + randomResult(1).toString + "," + randomResult(2).toString + "," + randomResult(3).toString + ","
        resultString
      }
    }

  }
  // getRandomCategory: Will return a string of a random Category
  def getRandomCategory(vector: Vector[Row], spark: SparkSession): String ={
    val randomNum = rs.nextInt(vector.length-1)
    val randomResult = vector(randomNum)
    val resultString = randomResult(2).toString
    resultString
  }
}
