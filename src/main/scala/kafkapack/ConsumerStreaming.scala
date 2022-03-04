package kafkapack
import contextpack._
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer 
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe



object ConsumerStreaming {
  //read from kafka 
      val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topicA", "topicB")
  val stream = KafkaUtils.createDirectStream[String, String](
    // StreamingContext below, get current running StreamingContext imported from context package
    MainContext.getStreamingContext(),
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )


    





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
}

