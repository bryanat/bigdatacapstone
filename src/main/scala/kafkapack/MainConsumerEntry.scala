package kafkapack

object MainConsumerEntry {

<<<<<<< HEAD
  def main(args: Array[String]): Unit = {
    println("MainConsumerEntry started...")

    ConsumerStreaming.readFromSource("topic1")

    //// cannot save to a variable: no output operation so nothing to execute error
    // val consumer = ConsumerStreaming2("topic1")
  }
=======
  
    def main(args: Array[String]): Unit = {
        println("MainConsumerEntry started...")
        
        //ConsumerStreaming.readFromSource("topic1")
         
        ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse", "ec2-3-81-9-55.compute-1.amazonaws.com:9092"))
        ////cannot save to a variable: no output operation so nothing to execute error
        //val consumer = ConsumerStreaming2("topic1")
     

    }
>>>>>>> refs/remotes/origin/kafka/bryan
}
