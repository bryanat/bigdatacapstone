package kafkapack

object MainProducerEntry {
  
    def main(args: Array[String]): Unit = {
        println("MainProducerEntry started...")
        
        ProducerStreaming.streamFromSource("topic1")

    }
}
