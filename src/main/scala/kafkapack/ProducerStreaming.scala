package kafkapack
import contextpack._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig, RecordMetadata}
import java.util.concurrent.Future
import java.util.Properties
import scala.collection.mutable.HashMap


object ProducerStreaming {


  def streamFromSource(topic: String): Unit ={


  //create streaming source
  val ssc = MainContext.getStreamingContext()
  val dstream = ssc.textFileStream("file:///C:/Users/joyce/IdeaProjects/bigdatacapstone/dataset-online/dstream")

  //Producer team will stream their line by line stream data to socketTextStream("ec2-3-81-9-55.compute-1.amazonaws.com", 9092)
  // Create a DStream that will connect to hostname:port, like localhost:9999
  //val lines = ssc.socketTextStream("localhost", 9999)


  //checkpoint: restarting point
  //ssc.checkpoint("checkpoint-directory")


  //set KafkaProducer properties
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:9092,anotherhost:9092")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put("producer.type", "async")
  //props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000")
  //props.put(ProducerConfig.BATCH_SIZE_CONFIG, "49152")


  //create an instance of broadcast Kafka producer
  val kafkasink = ssc.sparkContext.broadcast(KafkaSink(props))
  val now = System.currentTimeMillis()
  println(s"(Producer) Current unix time is: $now")


  //send the producer message with respect to a particular topic 
  dstream.foreachRDD ({ rdd =>
    println("(Producer) inside rdd is running")
    rdd.foreachPartition ({ records =>
      println("(Producer) inside record partition is running")
      // val metadata: List[Future[RecordMetadata]] = records.map { record => {
      //   kafkasink.value.send(topic, record)
      // }.toList
      //metadata.foreach(metadata=>println(metadata.value())
      

      records.foreach({message => 
      println("(Producer) inside partitioned record is running")
        val metadata = kafkasink.value.testsend(topic, message)
        println(metadata.topic())
        kafkasink.value.send(topic, message)
        println(message)
    })
    })
  })
  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate

  }

}
