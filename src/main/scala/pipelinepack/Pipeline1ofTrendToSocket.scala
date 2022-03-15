package pipelinepack

import socketpack.SocketServerPart
import producerpack.MainProducer

object Pipeline1TrendToSocket {
  def main(args: Array[String]) = {
    println("Pipeline1TrendToSocket started...")

    ///Socket, start server socket for client sockets to connect to
    SocketServerPart

    ////Producer, start multithreaded trend generator to create data streams
    MainProducer.startMainProducer()
  }
}