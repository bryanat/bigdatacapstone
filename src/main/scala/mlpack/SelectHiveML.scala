package mlpack

import contextpack._
import org.apache.spark._
import org.apache.spark.sql._

object SelectHive {
    def select(): Unit = {

    val warehouseLocation = "file:///C:/Users/joyce/IdeaProjects/bigdatacapstone/spark-warehouse"
    //val warehouseLocation = "hdfs://44.195.89.83:9000/user/hive/warehouse"


    val sparkConf = new SparkConf()
      .set("spark.sql.warehouse.dir", warehouseLocation)
      .set("spark.sql.catalogImplementation","hive")
      .setMaster("local[*]")
      .setAppName("p3")

    val ssql = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.executor.memory", "48120M") 
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()


      ssql.sql("SELECT * FROM hivetable").show()
    }
  
}
