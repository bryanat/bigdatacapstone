package consumerpack
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object testProducer extends App {

  def producerTest(): Unit = {

    val topicName = "test_1"

    val producerProperties = new Properties()
    producerProperties.setProperty(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
    )
    producerProperties.setProperty(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
    )
    producerProperties.setProperty(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    )

    val producer = new KafkaProducer[Int, String](producerProperties)

    producer.send(new ProducerRecord[Int, String](topicName, "message1"))
    producer.send(new ProducerRecord[Int, String](topicName, "message2"))
    producer.send(new ProducerRecord[Int, String](topicName, "message3"))
    producer.send(new ProducerRecord[Int, String](topicName, "message4"))
    producer.send(new ProducerRecord[Int, String](topicName, "message5"))
    producer.send(new ProducerRecord[Int, String](topicName, "message6"))

    producer.flush()

  }
}