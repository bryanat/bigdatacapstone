package producerpack

import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.Breaks.break

object MaxTest {
  //TRY a method that returns the maximum value for price which is in column 3  from the vector
  def getMaxPrice(spark: SparkSession) = {
    val df = spark.read.csv("dataset-online/productdata.csv")
    //Price vector first row contains a string "price" from header resulting in error during math comparison "<" & ">"
    //there is one string and a ton of integers get rid of one string before creating the vector
    //question for Cameron: can we do .toDouble ???

    val productVector = DataCollection.getProductDataList(spark)
    val listOfPrices = ListBuffer("1.0")
    var tempPrice = ""

    for (i <- 0 until productVector.length - 1) {
      tempPrice = productVector(i).get(3).asInstanceOf[String]
      listOfPrices += tempPrice
    }
    listOfPrices.remove(1)
    var listOfPricesDoubles = listOfPrices.map(x => x.toDouble)
    val maxPrice = listOfPricesDoubles.max


    //FILTERING SYSTEM FOR FINDING ITEMS WORTH > $500
    //I WILL SEPARATE THIS AND MAKE IT DYNAMIC SOON (HOPEFULLY!)
    var indexKeeper = ListBuffer[Int]()
    var highEndProductList = ListBuffer[Row]()

    for (i <- 1 to productVector.length - 2) {
        if (listOfPrices(i).toDouble > 500){
          indexKeeper += i
        }
      indexKeeper.foreach(x => highEndProductList += productVector(x))
    }
    val highEndVector = highEndProductList.toVector
    println(maxPrice)
    highEndVector.foreach(println)
  }
}
