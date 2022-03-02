package kafkapack

import contextpack.MainContext

object IDK {
  def startIDK(): Unit = {
    println("IDK started...")
    var sc = MainContext.getSparkContext()
    //error happening
    var filecontents = sc.textFile("file:///C:\\Users\\joyce\\IdeaProjects\\bigdatacapstone\\dataset-online\\dstreams\\empty-file-just-to-force-gen-dstreams-folder-in-git-push.csv")
    println(filecontents.collect().mkString(","))
    //var path = "$TROJAN_HOME/dataset-online/dstreams"
    //DStream from monitored file directory
  //var dstream = MainContext.getStreamingContext().textFileStream("file:///home/bryanat/bigdatacapstone/dataset-online/dstreams")
  //dstream.print()
    // Split each line into words
    //val dstream_words = dstream.flatMap(_.split(","))
  }
}
