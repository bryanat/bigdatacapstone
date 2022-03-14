package consumerpack
import kafkapack._

object MainConsumer {
  
  def  main(args:Array[String]): Unit = {
    println("Main Consumer started...")  
    SelectHive.select()

  }
}
