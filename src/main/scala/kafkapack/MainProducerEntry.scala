package kafkapack

object MainProducerEntry {
  
    def main(args: Array[String]): Unit = {
        println("MainProducerEntry started...")
        
        //////////////our team's topic, our team's broker address, our teams' socket ip and port/////////////////
        //ClickstreamKafkaProducer.producerKafka(Array("trojanhorse", "44.195.89.83:9000","localhost", "6666"))
        ClickstreamKafkaProducer.producerKafka(Array("trojanhorse", "localhost:9092","localhost", "6666"))

    }
}
