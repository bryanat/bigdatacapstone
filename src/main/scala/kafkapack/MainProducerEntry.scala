package kafkapack

object MainProducerEntry {
  
    def main(args: Array[String]): Unit = {
        println("MainProducerEntry started...")
        
        //ProducerStreaming.streamFromSource("topic1")
        ClickstreamKafkaProducer.producerKafka(Array("trojanhorse", "localhost:9092"))

    }
}
