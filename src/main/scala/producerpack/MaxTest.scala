package producerpack
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

object MaxTest {
  //TRY a method that returns the maximum value for price which is in column 3  from the vector
  val dc = new DataCollection



  def getMaxPrice(spark: SparkSession) = {
    val df = spark.read.csv("dataset-online/productdata.csv")
     val productVector = dc.getProductDataList(spark)
    val listOfPrices = ListBuffer("1.0")
    var tempPrice = ""

    for (i <- 0 until productVector.length - 1) {
      tempPrice = productVector(i).get(3).asInstanceOf[String]
      listOfPrices += tempPrice
    }
    listOfPrices.remove(1)
    //this removes the header row
    var listOfPricesDoubles = listOfPrices.map(x => x.toDouble)
    val maxPrice = listOfPricesDoubles.max
    println(maxPrice)
  }
}
