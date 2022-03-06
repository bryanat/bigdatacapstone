package kafkapack

object MainKafka2 {
  
    def main(args: Array[String]): Unit = {
        
        ConsumerStreaming.readFromSource("topic1")


    }
}
