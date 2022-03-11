package producerpack

import org.apache.spark.sql.SparkSession

object MainProducer {

    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def startMainProducer(): Unit = {

        val trans = new Transactions
        val rs = new RandomSelections

        println("Main Producer started...")
        var trend1Vector = trend1.getTrend1(spark, 400)
        trend1Vector.foreach(println)
        var randomVector = trans.getRandomTransactions(rs, spark, 500)
        randomVector.foreach(println)
        var trend2Vector = trend2.getTrend2(spark, 150)
        trend2Vector.foreach(println)
        var trend3Vector = trend3.getTrend3(spark, 500)
        trend3Vector.foreach(println)
        var blackFriday = blackFridayTrend.getBlackFridayTrend(spark, 500)
        blackFriday.foreach(println)
    }

}