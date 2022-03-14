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
        var trend2Vector = trend2.getTrend2(spark, 150)
        trend2Vector.foreach(println)
        var trend3Vector = trend3.getTrend3(spark, 500)
        trend3Vector.foreach(println)
        var trend4Vector = trend4.getTrend4(spark, 500)
        trend4Vector.foreach(println)
        var trend8Vector = trend8.getTrend8(spark, 1000)
        trend8Vector.foreach(println)
        var randomVector = randomData.getRandomData(spark, 4000)
        randomVector.foreach(println)
    }

}