package kafkapack

import contextpack._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata, ProducerConfig}
import java.util.concurrent.Future
import java.util.Properties

object TestProducer {

  def setProducer(): Unit = {

    var ssc = MainContext.getStreamingContext()
  
    val props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  
  //   val producer = new KafkaProducer[String, String](props);
    val producer = new KafkaProducer[String, String](props);
    val dstream = ssc.textFileStream("file:///home/bryanat/bigdatacapstone/dataset-online/dstreams")
  
    dstream.foreachRDD(rdd => {

      val now = System.currentTimeMillis()
      println(s"Current unix time is: $now")
  
      //println(rdd)
  
      rdd.foreachPartition {partitions =>
          val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
          partitions.foreach((line: String) => {
          try {
            var topicName = "urmom"
            // SEND TO TOPIC AS PRODUCERRECORD
            producer.send(new ProducerRecord[String, String]("urmom", line))
            println(line.length)
            println(s"Producer has published to topic: $topicName")
            println(line)
          } catch {
          case ex: Exception => {
              println("didn't suceed")
          }
      }
  })
  }
  })
//producer.close()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
}

// def send(topic: String, key: K, value: V): Future[RecordMetadata] =
//     producer.send(new ProducerRecord[K, V](topic, key, value))
//   for(int i = 0; i < 100; i++)
//       producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 
//    producer.close();

//     private val props = new Properties()

//   props.put("compression.codec", DefaultCompressionCodec.codec.toString)
//   props.put("producer.type", "sync")
//   props.put("metadata.broker.list", brokerList)
//   props.put("message.send.max.retries", "5")
//   props.put("request.required.acks", "-1")
//   props.put("serializer.class", "kafka.serializer.StringEncoder")
//   props.put("client.id", UUID.randomUUID().toString())

  
}
