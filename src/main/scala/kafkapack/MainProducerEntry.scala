package kafkapack

object MainProducerEntry {
  
    def main(args: Array[String]): Unit = {
        println("MainProducerEntry started...")
        
        //ProducerStreaming.streamFromSource("topic1")
        ClickstreamKafkaProducer.producerKafka(Array("trojanhorse", "ec2-3-81-9-55.compute-1.amazonaws.com:9092"))

    }
}
