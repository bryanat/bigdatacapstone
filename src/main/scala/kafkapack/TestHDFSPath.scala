package kafkapack
import contextpack._

object TestHDFSPath {

    def readFromHDFS(): Unit = {
        val ssql = MainContext.getSparkSession()
        val path = "hdfs://44.195.89.83:9000/tmpfiles/sample-of-final-data.csv"
        val data = ssql.read.textFile(path).collect()
        println(data.mkString)

    }
  
}
