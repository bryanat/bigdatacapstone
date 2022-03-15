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

  ///////if producer streaming does not register data, check dstream path and make sure the files are newly modified/////////////////////
  
  val topic = args(0)
  val brokers = args(1)
  val ip = args(2)
  val port = args(3).toInt

  
  val ssc = MainContext.getStreamingContext()
  val dstream = ssc.textFileStream("file:///C:/Users/joyce/IdeaProjects/bigdatacapstone/dstream1")
  //Producer team will stream their line by line stream data to socketTextStream("44.195.89.83", 9092)
  //val dstream = ssc.socketTextStream(ip, port)
  
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
    println("(Producer) inside rdd is running")
    rdd.foreachPartition ({ records =>
      println("(Producer) inside record partition is running")
      
      records.foreach({message => 
        println("(Producer) inside partitioned record is running")
        // val metadata = kafkasink.value.testsend(topic, message)
        // println(metadata.topic())
        kafkasink.value.send(topic, message)
        println(message)
        //System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - now));
    })
    })
  })
  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
  
}
  
}


  

// val rnd = new Random()
// val props = new Properties()
// props.put("metadata.broker.list", brokers)
// //props.put("serializer.class", "kafka.serializer.StringEncoder")
//  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
// props.put("producer.type", "async")
// val metadata: List[Future[RecordMetadata]] = records.map { record => {
//   kafkasink.value.send(topic, record)
// }.toList
//metadata.foreach(metadata=>println(metadata.value())