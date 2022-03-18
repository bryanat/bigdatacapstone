package consumerpack
import contextpack._
import org.apache.spark._
import org.apache.spark.sql._

object SelectHive {
    def select(): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val warehouseLocation = "file:///C:/Users/joyce/IdeaProjects/bigdatacapstone/spark-warehouse"
    //val warehouseLocation = "hdfs://44.200.236.7:9000/user/hive/warehouse"


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
      //.config("hive.metastore.uris", "thrift://44.200.236.7:9000")
      .enableHiveSupport()
      .getOrCreate()

      ssql.sql("SELECT * FROM hivetable1").show(100)
      ssql.sql("select order_id, payment_txn_id, product_name, price, product_category, city, country from hivetable  where payment_txn_success = 'N' order by country asc, city asc").show(100)


      

    }
  
}
