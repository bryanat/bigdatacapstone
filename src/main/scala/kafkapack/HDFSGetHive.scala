package kafkapack
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import scala.collection.immutable.Stream

object HDFSGetHive {
   def getFromHDFS(): Unit = {
    val hdfs = FileSystem.get(new URI("hdfs://44.195.89.83:50070"), new Configuration()) 
    val path = new Path("/tmpfiles/sampledata10.txt")
    val stream = hdfs.open(path)

    
    //def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))

    //This example checks line for null and prints every existing line consequentally
    //readLines.takeWhile(_ != null).foreach(line => println(line))
   }
}
