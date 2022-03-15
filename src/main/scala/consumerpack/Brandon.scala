package consumerpack

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.types._


object Brandon {
//Initiate Spark Session

  // def main(args: Array[String]): Unit = {
  def oldMain(): Unit = {


   System.setProperty("hadoop.home.dir", "c:/hadoop")
   val spark = SparkSession
     .builder()
     .appName("project1")
     .config("spark.master", "local")
     .getOrCreate()



    val df_Pthree = spark.read.option("header","true").csv("dataset-online/temp-data.csv")
    var df = df_Pthree.select(
      "order_id",
      "customer_id",
      "product_id",
      "product_name",
      "product_category",
      "payment_type",
      "price",
      "datetime",
      "country",
      "city",
      "ecommerce_website_name",
      "payment_txn_id",
      "payment_txn_success")

    // -----------------------------------------------------------------------------------------
    // BRANDON DF SUMMARIZE STATEMENTS BELOW
    // 1) Most Common payment Methods SELECT payment_type, COUNT(payment_txn_id) FROM ??? GROUP BY payment_type ORDER BY COUNT(payment_txn_id) DESC
    println("1) Most Common payment Methods")
    df.groupBy("payment_type").count().orderBy(col("count")).withColumnRenamed("count", "payment_method").show(999)
    /*2) Avg purchase cost per person, grouped by ecommerce site
    SELECT AVG(price), customer_id, ecommerce_website_name FROM ???
      WHERE payment_txn_success = 'Y' GROUP BY customer_id, ecommerce_website_name*/
    println("2) Avg purchase cost per person, grouped by ecommerce site")
//  val df_price = df.withColumn("price",col("price").cast(DecimalType(2,2)))
val df_price = df.withColumn("price", round(col("price"), 2))

  df_price.where(df("payment_txn_success") === "Y")
      .groupBy("ecommerce_website_name")
      .avg("price")
      .withColumnRenamed("avg(price)", "cost_per_person")
      .orderBy(col("cost_per_person"))
      .show(999)

  println("2a) possible further breakdown by location")
  df_price.where(df("payment_txn_success") === "Y")
    .groupBy("ecommerce_website_name", "country")
    .avg("price")
    .withColumnRenamed("avg(price)", "cost_per_person")
    .orderBy(col("cost_per_person").desc, col("country").asc)
    .show(999)
    /*
SELECT AVG(price), ecommerce_website_name, country
FROM ???
WHERE payment_txn_success = 'Y'
GROUP BY customer_id, ecommerce_website_name, country

 // order_id | customer_id | customer_name
// product_id | product_name | product_category
// payment_type* | qty | price
// datetime | country | city
// ecommerce_website_name
// payment_txn_id | payment_txn_success | failure_reason
 */




 }
}