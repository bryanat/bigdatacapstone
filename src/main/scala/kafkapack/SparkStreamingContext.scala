package kafkapack

import contextpack.MainContext

object SparkStreamingContext {
  def startSparkStreamingContext(): Unit = {
    println("IDK started...")
    //var sc = MainContext.getSparkContext()
    //error happening
    //var filecontents = sc.textFile("/home/bryanat/bigdatacapstone/dataset-online/dstreams/empty-file-just-to-force-gen-dstreams-folder-in-git-push.csv")
    //println(filecontents.collect().mkString(","))
    //var path = "$TROJAN_HOME/dataset-online/dstreams"
    //DStream from monitored file directory
    //MainContext.getStreamingContext()
  var ssc = MainContext.getStreamingContext()
  var dstream = ssc.textFileStream("file:///home/bryanat/bigdatacapstone/dataset-online/dstreams")
  /////////produce
  /////////consumer
  dstream.print()

  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate

    // Split each line into words
    //val dstream_words = dstream.flatMap(_.split(","))
  }
}
