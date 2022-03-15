package kafkapack
import contextpack._
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer 
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.TaskContext
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import java.io._
import org.apache.hadoop.fs.{FileSystem, Path}

object ConsumerStreaming {

  def readFromSource(topic: String): Unit = {
    
    
    val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
     )

    //topics has to be Array type, not Strings
    val topics = Set(topic)
    val ssc = MainContext.getStreamingContext()
    val topicdstream = KafkaUtils.createDirectStream[String, String](
      // StreamingContext below, get current running StreamingContext imported from context package
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

      
    val now = System.currentTimeMillis()
    println(s"(Consumer) Current unix time is: $now")

    
    val results = topicdstream.map(record=>Tuple2(record.key(), record.value()))
    val lines = results.map(_._2)
    lines.foreachRDD {rdd => 
      val testrdd = rdd.collect()
      println(testrdd.mkString)
      
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    

    def temp(): Unit = {
     var prevrdd =SparkContext.getOrCreate.emptyRDD[String]
    // //windowStream method
    //  val windowStream = topicdstream.window(Minutes(1))
    //  windowStream.foreachRDD{rdd => rdd.foreach{
    //  record => 
    // val sc = SparkContext.getOrCreate()
    //  val value = record.value()
    //  prevrdd = prevrdd.union(sc.parallelize(List(value)))}
    //  }

    // val array = new ListBuffer[String]()


    //import ssess.implicits._
    //var file = "C:/Users/joyce/IdeaProjects/bigdatacapstone/dataset-online/data/test.txt"
    topicdstream.foreachRDD {rdd => 
      rdd.foreach { record =>
        //.value() returns deserialized value column
        val sc = SparkContext.getOrCreate()
        val value = record.value()
        //parallelize value into rdd
        prevrdd = sc.parallelize(List(value)).union(prevrdd)
        prevrdd.coalesce(1).saveAsTextFile("file:///C:/Users/joyce/IdeaProjects/bigdatacapstone/dataset-online/data"+sc.applicationId+"/"+ System.currentTimeMillis())
      //   val filepath = "hdfs://namenode_ip:port/data/"+sc.applicationId+"/"+ System.currentTimeMillis()
      //   prevrdd.coalesce(1).saveAsTextFile(filepath)
      //   val fs = FileSystem.get(sc.hadoopConfiguration)
      //   val deletepath = new Path(filepath)
      //   if (fs.exists(deletepath)) {
      //       fs.delete(deletepath, true) }
      // }
      println(value)
    }
    //   rdd.foreach { record =>
    //   val value = record.value()
    //   array += value
    //   println(array.mkString("\n"))
    //   }
     }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    
    //array.toList
    }
  }
  
}


//testing offsets
// topicdstream.foreachRDD { rdd => 
  //   val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  //   rdd.foreachPartition { iter =>
    //     val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//     println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//   }
// }

// Import dependencies and create kafka params as in Create Direct Stream above

// val offsetRanges = Array(
//   // topic, partition, inclusive starting offset, exclusive ending offset
//   OffsetRange(topic, 0, 0, 100)
// )

// val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, PreferConsistent)
//Streams can be very easily joined with other streams.
// val stream1: DStream[String, String] = ...
// val stream2: DStream[String, String] = ...
// val joinedStream = stream1.join(stream2)


//it is often very useful to do joins over windows of the streams. 
// val windowedStream1 = stream1.window(Seconds(20))
// val windowedStream2 = stream2.window(Minutes(1))
// val joinedStream = windowedStream1.join(windowedStream2)


//Here is yet another example of joining a windowed stream with a dataset.
//  val dataset: RDD[String, String] = ...
// val windowedStream = stream.window(Seconds(20))...
// val joinedStream = windowedStream.transform { rdd => rdd.join(dataset) }

//// Reduce last 30 seconds of data, every 10 seconds
//val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))
    //val sconf = MainContext.getSparkConf()
     //val sc = new SparkContext(sconf)
   //  val ssc = new StreamingContext(sc, Seconds(2))
   // ssc.sparkContext.setLogLevel("ERROR")
    //val ssess = SparkSession.builder.config(sc.getConf).getOrCreate()
    // val ssess = MainContext.getSparkContext()