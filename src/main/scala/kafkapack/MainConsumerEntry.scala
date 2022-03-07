package kafkapack

object MainConsumerEntry {

  
    def main(args: Array[String]): Unit = {
        println("MainConsumerEntry started...")
        
        //ConsumerStreaming.readFromSource("topic1")
        //creates a list of messages in string
        // while (true) {
        // try {
        val consumer = ConsumerStreaming2("topic1")
        // }
        // catch {
        //     case e: java.lang.IllegalArgumentException => println("no message sent")
        // }
    //}


    }
}
