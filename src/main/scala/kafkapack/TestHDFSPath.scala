package kafkapack
import contextpack._

object TestHDFSPath {

    def writeToHDFS(): Unit = {
        val ssql = MainContext.getSparkSession()
        val path = "hdfs://44.195.89.83:9000/tmpfiles/sampledata10.txt"
        val data = ssql.read.textFile(path).collect()
        println(data.mkString)


    }
  
}
