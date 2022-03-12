package kafkapack

/* The purpose of this file is to practice and create a test Connection
 * Using Akka. Once connection is established - code will be integrated in 
 * KafkaAkka.scala */

import akka.stream._
import akka.stream.scaladsl._
import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.util.ByteString
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths

object TestAkka extends App {
    implicit val system: ActorSystem = ActorSystem("QuickStart")
//   Code here
    val source: Source[Int, NotUsed] = Source(1 to 100)
    val done: Future[Done] = source.runForeach(i => println(i))
    implicit val ec = system.dispatcher
    done.onComplete(_ => system.terminate())

    
}
