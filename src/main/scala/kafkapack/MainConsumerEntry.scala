package kafkapack

object MainConsumerEntry {

  
    def main(args: Array[String]): Unit = {
        println("MainConsumerEntry started...")
    
         ///////////// other teams topic, other team's broker address, our team's hive table location //////////////////////////////////
        //ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","44.195.89.83:9000", "hdfs://44.195.89.83:9000//remotedir"))
        ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","localhost:9092", "file:///C:/Users/joyce/IdeaProjects/bigdatacapstone/spark-warehouse"))



        //test path
        //HDFSGetHive.getFileFromHDFS()
        //TestHDFSPath.readFromHDFS()

    }

}
