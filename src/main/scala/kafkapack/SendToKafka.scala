package kafkapack

//import org.apache.kafka.clients.producer._
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


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
      producer.send(new ProducerRecord[String, String]("testtopic", line))
    } catch {
      case ex: Exception => {
        log.warn(ex.getMessage, ex)
      }
    }
  })
})
*/


}
