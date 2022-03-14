package kafkapack
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata

class KafkaDStreamExceptionHandler extends Callback {

    val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = 
    lastException.set(Option(exception))

  def throwExceptionIfAny(): Unit = 
    lastException.getAndSet(None).foreach(e => throw e)

  
}
