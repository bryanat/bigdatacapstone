package consumerpack

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization._
import java.util.Properties

import org.apache.spark.sql._

object YashConsumer {
    def main(args: Array[String]) = {
        /*
        val props = new Properties()
        props.put("bootstrap.servers", "SERVER DETAILS HERE")
        props.put("group.id", "CONSUMER GROUP NAME")
        props.put("key.deserializer", classOf[StringDeserializer])
        props.put("value.deserializer", classOf[StringDeserializer])

        val kafkaConsumer = new KafkaConsumer[String, String](props)
        kafkaConsumer.subscribe(COLLECTION_OF_TOPICS)

        while (true) {
            val result = kafkaConsumer.poll(2000).asScala
            for ((topic, data) <- result) {
                //OPERATIONS THAT WILL OCCUR WHEN STREAMING DATA
            }
        }
        */

        System.setProperty("hadoop.home.dir", "c:/winutils")
        val spark = SparkSession
          .builder()
          .appName("bigdatacapstone")
          .config("spark.master", "local")
          .enableHiveSupport()
          .getOrCreate()

        val df = spark.read.csv("dataset-online/samplecsv.csv")
        df.show();


    }
}
