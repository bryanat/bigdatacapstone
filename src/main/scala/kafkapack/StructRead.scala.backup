package kafkapack
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._

import contextpack._

// import scala.collection.JavaConverters._
// import org.apache.kafka.clients.consumer.KafkaConsumer
// import org.apache.kafka.common.serialization.StringDeserializer 



object StructRead {

def subscribe(): Unit = {

    val sc = MainContext.getSparkSession()
    import sc.implicits._

    // ds returns the kafka schema for topic with key, value columns serialized
    val ks= sc
    .readStream 
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscribe", "source-topic1")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()


    //dsStruct returns the deserialized key and value columns
    //val dsStruc = ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")   //.as[(STRING, STRING)]

    //qtopic returns the deserialized value column
    val valueCol = ks.selectExpr("CAST(value AS STRING)")


//method 1: 


  //valueColS is value column saved to memory: optional
    // val valueColS = valueCol.
    //     writeStream 
    //     .queryName("qtopicS")
    //     .format("memory")
    //     .start()

  //transform value column into RDD
    //val topicrdd = valueCol.rdd

  //transform rdd into arrays
    //val topicarray = topicrdd.collect()

  //scroll down to middle of this page to read why converting array to dataframe is not advised: https://mtpatter.github.io/bilao/notebooks/html/01-spark-struct-stream-kafka.html



//method 2:

  //converts deseriazlied values columns directly into dataframe bypassing rdd
    val topicDF = valueCol
    .selectExpr("split(value, ',')[0] as product_id",
    "split(value, ',')[1] as product_name",
    "split(value, ',')[2] as product_category",
    "split(value, ',')[3] as price")
    topicDF.show()
}
  
}
