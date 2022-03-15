package kafkapack
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig, RecordMetadata}
import scala.util.Random
import java.util.Date
import java.util.concurrent.Future
import java.util.Properties
import scala.collection.mutable.HashMap
import contextpack._




object ClickstreamKafkaProducer extends App{

  def producerKafka(args: Array[String]): Unit = {
  
  val topic = args(0)
  val brokers = args(1)

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val ssc = MainContext.getStreamingContext()
  val dstream = ssc.textFileStream("file:\\C:/Users/joyce/IdeaProjects/bigdatacapstone/dstream1")
  //Producer team will stream their line by line stream data to socketTextStream("44.195.89.83", 9092)

   val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put("producer.type", "async")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  // props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000")
  // props.put(ProducerConfig.BATCH_SIZE_CONFIG, "49152")

  //create an instance of broadcast Kafka producer
  val kafkasink = ssc.sparkContext.broadcast(KafkaSink(props))
  val now = System.currentTimeMillis()
  println(s"(Producer) Current unix time is: $now")

  //send the producer message with respect to a particular topic 
  dstream.foreachRDD ({ rdd =>
      // kafkasink.value.send(topic, "tebbles")
      // val metadata = kafkasink.value.testsend(topic, "tebbles")
      // println(metadata.topic())
    rdd.foreachPartition ({ records =>
      
      records.foreach({message => 
        // val metadata = kafkasink.value.testsend(topic, message)
        // println(metadata.topic())
        kafkasink.value.send(topic, message)
        println(message)
        // println(s"Sent to topic $topic: $message")
        //System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - now));
    })
    })
  })
  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
  // val metadata: List[Future[RecordMetadata]] = records.map { record => {
  //   kafkasink.value.send(topic, record)
  // }.toList
  //metadata.foreach(metadata=>println(metadata.value())
  
}
  
}

