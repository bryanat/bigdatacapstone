package kafkapack

object MainConsumerEntry {

  
    // def main(args: Array[String]): Unit = {
    def oldMain(): Unit = {
        println("MainConsumerEntry started...")
    
         ///////////// other teams topic, other team's broker address, our team's hive table location //////////////////////////////////
        //ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","44.195.89.83:9000", "hdfs://44.195.89.83:9000//remotedir"))
        ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","localhost:9092", "/home/ubuntu/gen/bigdatacapstone/spark-warehouse"))



         //ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","44.200.236.7:9092",  "file:///C:/Users/joyce/IdeaProjects/bigdatacapstone/spark-warehouse"))
         //ClickstreamConsumerStreaming.consumerKafka(Array("Wednesday","3.86.155.113:9092",  "hdfs://44.200.236.7:9000/user/hive/warehouse"))
         //ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","44.200.236.7:9092",  "hdfs://44.200.236.7:9000/user/hive/warehouse"))

         //test path
         //HDFSGetHive.getFileFromHDFS()
         //TestHDFSPath.readFromHDFS()
         
         
        }

}
