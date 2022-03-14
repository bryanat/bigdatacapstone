package socketpack

import java.net._
import java.io._
import java.util.Random

class SocketWorking() {

  // // can only be called once
  // var serverSocket = new ServerSocket(6666)
  var serverSocket = SocketWorkingServerpart.serverSocket
  // // this .accept() method is key
  var clientSocket = serverSocket.accept()
  // var clientSocket = new Socket("localhost", 6666)
  // var socketAddr = new InetSocketAddress("localhost", 6666)
  // clientSocket.bind(socketAddr)
  var out = new PrintWriter(clientSocket.getOutputStream(), true)
  var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))


  // def sendOverSocket(data: String) = {
  //   out.println(data)
  // }

  // while (true) {
  //   var randomnum = new Random()
  //   var randomnumstring = randomnum.toString()
  //   println("looping... " + randomnumstring)


  //   out.println(/*TrendMaker DATA*/)
  //   out.println("XPHEOXXNAJSNAINSDI")
  //   Thread.sleep(300)
  // }
  // }

  // def stop(): Unit = {
  //   in.close()
  //   out.close()
  //   clientSocket.close()
  //   serverSocket.close()
  // }
}

