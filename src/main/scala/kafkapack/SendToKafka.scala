package kafkapack

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata, ProducerConfig}
import java.util.concurrent.Future
import java.util.Properties




object SendToKafka {
  ////HOW TO SEND DATA TO KAFKA
  //KafkaProducer.send()

  //HOW TO SEND DATA TO KAFKA
  //KafkaProducer.send(new ProducerRecord[String, String]("topic", line))

/*
requestSet.foreachPartition((partitions: Iterator[String]) => {
  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
  partitions.foreach((line: String) => {
    try {
      //var fields= selected fields from lines
      producer.send(new ProducerRecord[String, String]("testtopic", fields))
    } catch {
      case ex: Exception => {
        log.warn(ex.getMessage, ex)
      }
    }
  })
})
*/


// var ssc = MainContext.getStreamingContext()


// def setProducer(): Unit = {
//   val props = new Properties();
//   props.put("bootstrap.servers", "localhost:127.0.0.1:9092");
//   props.put("acks", "all");
//   props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//   props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 
//   val producer = new KafkaProducer[String, String](props);
//   val dstream = ssc.textFileStream("file:///home/bryanat/bigdatacapstone/dataset-online/dstreams")

// dstream.foreachRDD(rdd => {
//     rdd.foreachPartition {partitions =>
//         val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
//         partitions.foreach((line: String) => {
//         try {
//         producer.send(new ProducerRecord[String, String]("testtopic", line))
//         println("inside producer send")
        
//         } catch {
//         case ex: Exception => {
//             println("didn't suceed")
//         }
//     }
// })
// producer.close()
//     }
// })
// }


<<<<<<< HEAD
}
=======
}
>>>>>>> 7535e685694340f3920a9810de2177b187955e10
