package kafkapack

object MainConsumerEntry {
  
    def main(args: Array[String]): Unit = {
        println("MainConsumerEntry started...")
        
        ConsumerStreaming.readFromSource("topic1")

    }
}
