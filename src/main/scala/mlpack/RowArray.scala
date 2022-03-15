package mlpack
import contextpack._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object RowArray {
  val ssql = MainContext.getSparkSession()
    import ssql.implicits._


  
    //returns a scala array of ROW
    def getArray(starttime:String, endtime:String): Array[Row] = {
      //maps for categorical features (all labels and features must be Double type for dense/sparse vectors)
      //product category, payment type, country, city, website
      val productCategoryMap = Map("product1"->"1", "product2"->"2")
      val paymentTypeMap = Map("credit"->"1", "debit"->2)
      val paymentSucessMap = Map("n"->0 , "y"->1)
        //udfs that get year, month, day, time of day from datetime
        val gettimeudf = udf((datetime:String)=>"14")
        val getdayudf = udf((datetime:String)=>"12")
        val getyearudf = udf((datetime:String)=>"2022")
        val getmonthudf = udf((datetime:String)=>"11")
        val getcategoryudf = udf((category: String)=>productCategoryMap.getOrElse(category.toLowerCase(), 0))
        val gettypeudf = udf((payment: String)=>paymentTypeMap.getOrElse(payment.toLowerCase(), 0))
        val getsucessudf = udf((payment:String)=>paymentSucessMap.getOrElse(payment.toLowerCase(), 0))
        //so on...
        


        val rowArray = ssql.sql("SELECT customer_id, product_id, product_category, payment_type, qty, price, datetime, payment_txn_sucess FROM csmessages_hive_table").toDF("customer_id", "product_id", "product_category", "payment_type", "qty", "price", "datetime", "payment_txn_sucess")
        .filter("customer_id is not NULL").filter("product_id is not NULL").filter("qty is not NULL").filter("price is not NULL").filter("datetime is not NULL")
        .filter("payment_txn_sucess is not NULL")
        .withColumn("time", gettimeudf($"datetime")).withColumn("day",getdayudf($"datetime")).withColumn("month", getmonthudf($"datetime")).withColumn("year", getyearudf($"datetime"))
        .withColumn("category", getcategoryudf($"product_id"))
        .withColumn("payment_type", gettypeudf($"payment_type"))
        .withColumn("success", getsucessudf($"payment_txn_success"))
        .drop("datetime").drop("product_category").drop("payment_type")
        .collect()
        rowArray
    }


}
