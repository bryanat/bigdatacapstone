package pipelinepack

import kafkapack.ClickstreamConsumerStreaming

object Pipeline3KafkatopicToHivetable {
  def main(args: Array[String]) = {
    println("Pipeline3KafkatopicToHivetable started...")

    ///////////// other teams topic, other team's broker address, our team's hive table location //////////////////////////////////
    //ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","44.195.89.83:9000", "hdfs://44.195.89.83:9000//remotedir"))
    ClickstreamConsumerStreaming.consumerKafka(Array("trojanhorse","localhost:9092", "/home/bryanat/gitclonecleanbigdata/bigdatacapstone/spark-warehouse"))


  }
}