package producerpack

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.StdIn.readLine

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

      val tm = new TrendMaker
      val trans = new Transactions
      val rs = new RandomSelections

      println("Main Producer started...")
//        var trend1Vector = trend1.getTrend1(spark, 100)
//        trend1Vector.foreach(println)
//        var randomVector = trans.getRandomTransactions(rs, spark, 500)
//        randomVector.foreach(println)
//        var trend2Vector = trend2.getTrend2(spark, 150)
//        trend2Vector.foreach(println)
//        var trend3Vector = trend3.getTrend3(spark, 500)
//        trend3Vector.foreach(println)
      println("Generating Trend 1...")
      val t1 = trend1.getTrend1(spark, 1000)
      println("Generating Trend 2...")
      val t2 = trend2.getTrend2(spark, 1000)
      println("Generating Trend 3...")
      val t3 = trend3.getTrend3(spark,1000)
      println("Generating Trend 4...")
      val t4 = trend4.getTrend4(spark,1000)
      println("Generating Trend 5...")
      val t5 = trend5.getTrend5(spark, 1000)
      println("Generating Trend 6...")
      val t6 = trend6.getTrend6(spark, 1000)
      println("Generating Trend 7...")
      val t7 = trend7.getTrend7(spark, 1000)
      println("Generating Trend 8...")
      val t8 = trend8.getTrend8(spark, 1000)
      println("Generating Trend 9...")
      val t9 = ExponentialIncreaseOverTimeTrend.createTrend(1000, 1, 10,ListBuffer(1,1,2000))
      println("All trends generated. Loading into threads...")
      tm.loadData(t1)
      tm.loadData(t2)
      tm.loadData(t3)
      tm.loadData(t4)
      tm.loadData(t5)
      tm.loadData(t6)
      tm.loadData(t7)
      tm.loadData(t8)
      tm.loadData(t9)
      println("Data loaded into threads. Would you like to start the treads?")
      val cont = readLine("Y/N: ")
      if(cont.toLowerCase == "n"){
        return
      }
      tm.startTreads()
      while(tm.waitingThreads.length > 0){
        tm.startTreads()
      }
      println("All threads complete.")
    }

}