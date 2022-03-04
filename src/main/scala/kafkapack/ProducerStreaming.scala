package kafkapack
import contextpack._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig, RecordMetadata}
import java.util.Properties
import scala.collection.mutable.HashMap





object ProducerStreaming {

  def streamFromSource(topic: String): Unit ={

  //create streaming source
  val ssc = MainContext.getStreamingContext()
  //where is our source of streaming file?
  var dstream = ssc.textFileStream("file:///home/bryanat/bigdatacapstone/dataset-online/dstreams")

  //set KafkaProducer properties
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, NearLineConfig.kafka_brokers)
  //    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
  //    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put("producer.type", "async")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "49152")


  //create an instance of broadcast Kafka producer
  val kafkasink = ssc.sparkContext.broadcast(KafkaSink(props))

  //send the producer message with respect to a particular topic 
  dstream.foreachRDD { rdd =>
  rdd.foreachPartition { partitionOfRecords =>
    partitionOfRecords.foreach({message => 
      //regex/ pick your fields in record
        kafkasink.value.send(topic, message)
    })
    }
  }
  }

    //Finally, this can be further optimized by reusing connection objects across multiple RDDs/batches. One can maintain a static pool of connection objects than can be reused as RDDs of multiple batches are pushed to the external system, thus further reducing the overheads.
// dstream.foreachRDD { rdd =>
//   rdd.foreachPartition { partitionOfRecords =>
//     // ConnectionPool is a static, lazily initialized pool of connections
//     val connection = ConnectionPool.getConnection()
//     partitionOfRecords.foreach(record => connection.send(record))
//     ConnectionPool.returnConnection(connection)  // return to the pool for future reuse

}
