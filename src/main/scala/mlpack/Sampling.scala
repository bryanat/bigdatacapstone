package mlpack
import contextpack._

object Sampling {
  

    val ssql = MainContext.getSparkSession()
    import ssql.implicits._

    def stratifiedSampling(starttime: String, endtime: String, file: String): Unit={
        val df = ssql.sql("SELECT product_id, qty, price, datetime, payment_txn_sucess " +
          "FROM csmessages_hive_table").toDF("product_id", "qty", "price", "datetime", "payment_txn_sucess")
        //fractions is a map that specifies which percentage of classA fire to classG fire you want; it picks a percentage of sample for each key
        val fractions = Map("A"-> 0.01,"B"-> 0.05, "C"->0.1, "D"->0.2, "E"-> 0.4, "F"->0.5, "G"->0.8)
        //123 is the seed: if you want the same sample next time, use 123 again; if you want a different sample, use another seed: 456, 1234, 78, anything works. 
        val sample = df.stat.sampleBy("product_category", fractions, 123)
        sample.write.parquet(file)
    }

    def randomSampling(starttime: String, endtime:String, file:String): Unit={
        val df = ssql.sql("SELECT product_id, qty, price, datetime, payment_txn_sucess " +
          "FROM csmessages_hive_table").toDF("product_id", "qty", "price", "datetime", "payment_txn_sucess")
        //unlike the fractions in stratified sampling for each partition, the fraction in random sampling is applied to the entire dataset
        var fraction = 0.5
        //you can specify an optional seed after fraction
        val sample = df.sample(fraction)
        sample.write.parquet(file)
    }
  
}
