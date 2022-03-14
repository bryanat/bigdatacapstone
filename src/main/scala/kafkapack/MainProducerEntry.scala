package kafkapack

object MainProducerEntry {
<<<<<<< HEAD

  def main(args: Array[String]): Unit = {
    println("MainProducerEntry started...")
    
    ProducerStreaming.streamFromSource("topic1")
  }
=======
  
    def main(args: Array[String]): Unit = {
        println("MainProducerEntry started...")
        
        //ProducerStreaming.streamFromSource("topic1")
        ClickstreamKafkaProducer.producerKafka(Array("trojanhorse", "localhost:9092"))

    }
>>>>>>> refs/remotes/origin/kafka/bryan
}
