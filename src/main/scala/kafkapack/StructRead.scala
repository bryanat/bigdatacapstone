package kafkapack
import contextpack._

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer 

object StructRead {

def subscribe(): Unit = {

    val sc = MainContext.getSparkSession()
    import sc.implicits._

    val ds= sc
    .readStream // use `read` for batch, like DataFrame
    .format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscribe", "source-topic1")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()

    val dsStruc = ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
}
  
}
