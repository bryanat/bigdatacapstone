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
import org.apache.spark.rdd.RDD

class ConsumerStreaming2(createConsumerArray: ()=>RDD[String]) {

 lazy val stringArray = createConsumerArray()
 

//   def createDF(): DataFrame = {
        
//         //get sparkcontext and sparksession for parallelization
//         val ssess = MainContext.getSparkSession()

//         import ssess.implicits._
//         val rdd = ssess.sparkContext.parallelize(stringArray)
//         val df = rdd.toDF()
//         df
//     }
}

  object ConsumerStreaming2 {

    var arrayx = new ListBuffer[String]()
    
    // def getArray() = {
        //     arrayx
        // }
        
        def apply(topic: String): ConsumerStreaming2 = {
            
            val ssc = MainContext.getStreamingContext()


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
        val topicdstream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
        )

        
        val now = System.currentTimeMillis()
        println(s"(Consumer) Current unix time is: $now")
        
        //windowStream method
        // val windowStream = topicdstream.window(Minutes(1))
        // windowStream.transform{rdd => rdd.join(dataset)}
        
        var prevrdd =SparkContext.getOrCreate.emptyRDD[String]
        val arrayFunc = () => {
        //import ssess.implicits._
        topicdstream.foreachRDD {rdd => 
            rdd.foreach { record =>
            //.value() returns deserialized value column
             val sc = SparkContext.getOrCreate()
             val value = record.value()
             //parallelize value into rdd
             var valuerdd = sc.parallelize(List(value))
             //add current rdd to previous rdd
             val newrdd = prevrdd.union(valuerdd)
             //set prev rdd to curr rdd
            prevrdd = newrdd
            // arrayx += value
            // println(arrayx.mkString("\n"))
            }
        }
        //arrayx.toList
        prevrdd

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
        
