package mlpack
import contextpack._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object RowArray {
  val ssql = MainContext.getSparkSession()
    import ssql.implicits._


  
    //returns a scala array of ROW
    def getArray(starttime:String, endtime:String): Array[Row] = {
        //udfs that get year, month, day, time of day from datetime
        //val getyearudf = udf((datetime:String)=>regex to get year, needs to consider bad datatime)
        //val getmonthudf = udf((datetime:String)=>regex to get month, needs to consider bad datatime)
        //so on...
        

        //maps for categorical features (all labels and features must be Double type for dense/sparse vectors)
        //product category, payment type, country, city, website
        //val productCategoryMap = Map("product1"->"1", "product2"->"2", etc)
        //val paymentTypeMap = Map("credit"->"1", "debit"->2, so on...)

        val rowArray = ssql.sql("SELECT customer_id, product_id, product_category, payment_type, qty, price, datetime, payment_txn_sucess FROM csmessages_hive_table").toDF("customer_id", "product_id", "product_category", "payment_type", "qty", "price", "datetime", "payment_txn_sucess")
        .filter("customer_id is not NULL").filter("product_id is not NULL").filter("qty is not NULL").filter("price is not NULL").filter("datetime is not NULL")
        .filter("payment_txn_sucess is not NULL")
        //.withColumn($"time", getdayudf($"datetime")).withColumn($"day",getdayudf($"datetime")).withColumn($"month", getmonthudf($"datetime")).withColumn($"year", getyearudf($"datetime"))
        //.withColumn($"category", productCategoryMap.getOrElse($"product_id", 0))
        //.withColumn($"category", paymentTypeMap.getOrElse($"payment_type", 0))
        //.drop("datetime").drop("product_category").drop("payment_type")
        .collect()
        rowArray
    }

}
