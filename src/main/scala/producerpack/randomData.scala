package producerpack

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

object randomData {

  val TAG = "NOT"
  val rs = new RandomSelections
  val trans = new Transactions

  def getRandomData(spark: SparkSession, returnAmount: Int): Vector[String]={
    var orderCounter = 100000
    var orderID = TAG + orderCounter.toString
    var resultList = ListBuffer("")
    for (i <- 0 to returnAmount){
      val tempString = trans.createInitialTransaction(rs, spark, orderID,"All")
      resultList += tempString
      orderCounter = orderCounter+1
      orderID = TAG +orderCounter.toString
    }
    val resultVector = resultList.toVector
    resultVector
  }
}

