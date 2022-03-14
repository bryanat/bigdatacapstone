package socketpack

import java.net._
import java.io._

object SocketWorkingServerpart {
  var serverSocket = new ServerSocket(6666)

  // // this .accept() method is key
  println("before kafkaproducer socket")
  var clientSocketFORKAFKAPRODUCER = serverSocket.accept()
  // THE .accept() is for the KAFKAPRODUCER socket connection
  println("""
  XXX
  XXXX
  XXXXX
  after kafkaproducer socket
  XXXXX
  XXXX
  XXX
  """)

}
