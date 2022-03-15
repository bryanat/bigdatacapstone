package socketpack

import java.net._
import java.io._
import java.util.Random

class SocketWorking() extends Thread {

  var clientSocket = new Socket  

  override def run(): Unit = {
    /*
    def out(): PrintWriter = {
      // var out = new PrintWriter(clientSocket.getOutputStream(), true)
      new PrintWriter(clientSocket.getOutputStream(), true)
    }
    */
    var out = new PrintWriter(clientSocket.getOutputStream(), true)
    var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
  }

  // // can only be called once
  // var serverSocket = new ServerSocket(6666)

/* var serverSocket = SocketWorkingServerpart.serverSocket */

// // this .accept() method is key
/* var clientSocket = serverSocket.accept() */
  
  
  // var clientSocket = new Socket("localhost", 6666)

  // var socketAddr = new InetSocketAddress("localhost", 6666)
  // clientSocket.bind(socketAddr)


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

