package consumerpack
import kafkapack._

object MainConsumer {

  def startMainConsumer(): Unit = {
    println("Main Consumer started...")
    SelectHive.select()

  }
}
