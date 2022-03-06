package contextpack

// Spark deps
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._

object MainContext {
    
  def startMainContext(): Unit = {
    println("Main Context started...")

    getSparkConf()
    getSparkContext()
    getStreamingContext()
<<<<<<< HEAD
    // Spark log level set to not print INFO lines, accessed through the SparkContext (sc) "The associated SparkContext [sc beneath ssc] can be accessed using ssc.sparkContext ~= sc"
=======
    
    
>>>>>>> kafka/master
  }

  def getSparkConf(): SparkConf = {
    // may need to import team members's SPARK_HOME Path for .setSparkHome(/*SPARK_HOME_PATH*/) for each individual member's branches
    val sconf = new SparkConf().setMaster("local[*]").setAppName("P3").setSparkHome("C:\\Spark")
    sconf
  }

  def getSparkContext(): SparkContext = {
    // Spark SparkContext (sc) is main entrypoint for Spark API
    val sc = new SparkContext(getSparkConf())
<<<<<<< HEAD
=======
    // Spark log level set to not print INFO adn WARN lines, accessed through the SparkContext (sc) 
>>>>>>> kafka/master
    sc.setLogLevel("ERROR")
    sc
  }

  // Spark Streaming Context for Spark Streaming API
  def getStreamingContext(): StreamingContext = {
    // Spark StreamingContext (ssc) is main entrypoint for Spark Streaming API, built on top of SparkContext (sc)
    val ssc  = new StreamingContext(getSparkContext(), Seconds(2))
<<<<<<< HEAD
=======
    // Spark log level set to not print INFO adn WARN lines, accessed through the SparkContext (sc) "The associated SparkContext [sc beneath ssc] can be accessed using ssc.sparkContext ~= sc"
>>>>>>> kafka/master
    ssc.sparkContext.setLogLevel("ERROR")
    ssc
  }

   //Spark SQL context, SparkSession
   def getSparkSession(): SparkSession = {
    val sc = SparkSession.builder().appName("Pthree").config("spark.master", "local").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").enableHiveSupport().getOrCreate()
    sc
  }
}

/*
    // may need to import team members's SPARK_HOME Path for .setSparkHome(/*SPARK_HOME_PATH*/) for each individual member's branches
    val sconf = new SparkConf().setMaster("local[*]").setAppName("P3").setSparkHome("C:\\Spark")
    // SparkContext (sc) is main entrypoint for Spark API
    val sc   = new SparkContext(sconf)
    // StreamingContext (ssc) is main entrypoint for Spark Streaming API, built on top of SparkContext (sc)
    val ssc  = new StreamingContext(sc, Seconds(2))
    // Spark log level set to not print INFO lines, accessed through the SparkContext (sc) "The associated SparkContext [sc beneath ssc] can be accessed using ssc.sparkContext ~= sc"
    ssc.sparkContext.setLogLevel("ERROR")
    // Spark SQL context, SparkSession
    //val ssql = SparkSession.builder().appName("Wildfire").config("spark.master", "local").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").enableHiveSupport().getOrCreate()
*/