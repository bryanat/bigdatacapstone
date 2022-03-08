package producerpack

import org.apache.spark.sql.SparkSession

object MainProducer {

    def startMainProducer(): Unit = {
        System.setProperty("hadoop.home.dir", "c:/winutils")
        val spark = SparkSession
          .builder()
          .appName("project1")
          .config("spark.master", "local")
          .enableHiveSupport()
          .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val trans = new Transactions
        val rs = new RandomSelections

        println("Main Producer started...")
        var trend1Vector = trend1.getTrend1(100)
        trend1Vector.foreach(println)
        var randomVector = trans.getRandomTransactions(rs, spark, 500)
        randomVector.foreach(println)
    }
}