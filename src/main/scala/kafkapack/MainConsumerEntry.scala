package kafkapack

object MainConsumerEntry {

  def main(args: Array[String]): Unit = {
    println("MainConsumerEntry started...")

    ConsumerStreaming.readFromSource("topic1")

    //// cannot save to a variable: no output operation so nothing to execute error
    // val consumer = ConsumerStreaming2("topic1")
  }
}
