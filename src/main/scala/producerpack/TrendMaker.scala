package producerpack

import scala.collection.mutable.Stack
import scala.collection.mutable.ListBuffer
import contextpack.MainContext
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql._


class TrendThread(data:Vector[String]) extends Runnable{
  //this class is going to be used to make the threads to send data to the producer.
  //this is going to have a few functions.
  val dat = data

  override def run(): Unit = {

    println( Thread.currentThread().getName() + " is running a thread!")
    dat.foreach(p => println(p))
  }
}

class TrendMaker {
  //this value stores the maximum number of threads we want running at once
  val threadCount = 4
  //stores the threads that arent running
  var waitingThreads = Stack[Thread]()
  //stores the running threads
  var runningThreads = ListBuffer[Thread]()
  //first im just gonna test that my thread is working
  def loadData(data:Vector[String]):Unit= {
    //this adds the data in the thread to waiting threads
    waitingThreads.push(new Thread(new TrendThread(data)))
  }

  def startTreads():Unit= {
    //this starts the threads by first running through the currently running threads adn removing the dead ones.
    for(i<-0 until runningThreads.length){
      //if there is a running thread, then we want toi leave it alone. otherwise, we need to stop it
      //this can be removed
      if(!runningThreads(i).isAlive){
        runningThreads.remove(i)
      }
    }
    //add the new threads to running threads
    while(waitingThreads.nonEmpty && runningThreads.length < threadCount){
      runningThreads += waitingThreads.pop()
    }
    runningThreads.foreach(p => {if (!p.isAlive) p.start()})
  }
  def startAllWaitingThreads():Unit = {
    while(waitingThreads.nonEmpty){
      startTreads()
    }
  }
}
