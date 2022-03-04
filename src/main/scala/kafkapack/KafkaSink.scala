package kafkapack
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.collection.mutable.HashMap


//() => KafkaProducer is equivalent to createProducer[KafkaProducer]()
class KafkaSink(createProducer: ()=>KafkaProducer[String, String]) extends Serializable {
  //calls the object that creates a lazily evaluated producer
  lazy val producer = createProducer()

  //sends the producer record
  def send(topic: String, message: String): Unit= producer.send(new ProducerRecord(topic, message))

}
  



  //when KafkaSink object is called, apply method is implemented
  object KafkaSink {

    //converts Map properties to KafkaProducer properties
    import scala.collection.JavaConversions._

    def apply(props: HashMap[String, Object]): KafkaSink= {

        val producerFunction = () => {
            val producer = new KafkaProducer[String, String](props)
            //close kafka producer before shutdown of JVM so buffered messages are not lost
            sys.ShutdownHookThread {
            producer.close()
        }
        producer
    }
    new KafkaSink(producerFunction)
    }
  }


