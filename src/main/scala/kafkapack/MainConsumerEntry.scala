package kafkapack

object MainConsumerEntry {

  
    def main(args: Array[String]): Unit = {
        println("MainConsumerEntry started...")
    
         ///////////// other teams topic, other team's broker address, our team's hive table location //////////////////////////////////
        ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","44.200.236.7:9092",  "hdfs://44.200.236.7:9000/user/hive/warehouse"))
        //ClickstreamConsumerStreaming.consumerKafka(Array("LastTime2","3.86.155.113:9092",  "hdfs://44.200.236.7:9000/user/hive/warehouse"))
        //ClickstreamConsumerStreaming.consumerKafka(Array("Wednesday","3.86.155.113:9092",  "hdfs://44.200.236.7:9000/user/hive/warehouse"))
        //ClickstreamConsumerStreaming.consumerKafka(Array("trojanorse","localhost:9092", "hdfs://44.200.236.7:9000/user/hive/warehouse"))

        //test path
        //HDFSGetHive.getFileFromHDFS()
        //TestHDFSPath.readFromHDFS()


    }

}
