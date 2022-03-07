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

class ConsumerStreaming2(createConsumerArray: ()=>List[String]) {

 lazy val stringArray = createConsumerArray()
 

  def createDF(): DataFrame = {
        
        //get sparkcontext and sparksession for parallelization
        val sconf = MainContext.getSparkConf()
        val sc = new SparkContext(sconf)
        val ssess = SparkSession.builder.config(sc.getConf).getOrCreate()

        import ssess.implicits._
        val rdd = sc.parallelize(stringArray)
        val df = rdd.toDF()
        df
    }
}

  object ConsumerStreaming2 {

    var arrayx = new ListBuffer[String]()
    
    // def getArray() = {
        //     arrayx
        // }
        
        def apply(topic: String): ConsumerStreaming2 = {
            
            val ssc = MainContext.getStreamingContext()
        // val sconf = MainContext.getSparkConf()
        // val sc = new SparkContext(sconf)
        // val ssc = new StreamingContext(sc, Seconds(2))
        // ssc.sparkContext.setLogLevel("ERROR")
        // val ssess = SparkSession.builder.config(sc.getConf).getOrCreate()


        val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        //topics has to be Array type, not Strings
        val topics = Array(topic)
        //val ssc = MainContext.getStreamingContext()
        val topicdstream = KafkaUtils.createDirectStream[String, String](
        // StreamingContext below, get current running StreamingContext imported from context package
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
        )

        
        val now = System.currentTimeMillis()
        println(s"(Consumer) Current unix time is: $now")
        
        //windowStream method
        // val windowStream = topicdstream.window(Minutes(1))
        // windowStream.transform{rdd => rdd.join(dataset)}
        val arrayFunc = () => {
        //import ssess.implicits._
        topicdstream.foreachRDD {rdd =>
        //if 601 seconds then dont include in dataframe
            rdd.foreach { record =>
            val value = record.value()
            arrayx += value
            println(arrayx.mkString("\n"))
            }
        }
        arrayx.toList
        }
        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
        new ConsumerStreaming2(arrayFunc)
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
        