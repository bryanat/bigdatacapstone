package socketpack

import java.net._
import java.io._

object SocketWorkingServerpart {
  println("before ServerSocket")
  var serverSocket = new ServerSocket(6666)

  // // this .accept() method is key
  //var clientSocketFORKAFKAPRODUCER = serverSocket.accept()
  // THE .accept() is for the KAFKAPRODUCER socket connection
  println("""
  XXX
  XXXX
  XXXXX
  after ServerSocket
  XXXXX
  XXXX
  XXX
  """)

  // new thread for a client
//// new SocketWorking().start()
  println("after serversockets thread .start() method ")

  // another way of new thread for a client
  // var newSocketWorking = new SocketWorking()
  // newSocketWorking.start()

}
