package socketpack

import java.net._
import java.io._
import java.util.Random

class SocketServerHandler(port: Int) {

  // define global class types
  // var serverSocket: ServerSocket
  // var clientSocket: Socket
  // var out: PrintWriter
  // var in: BufferedReader

  // def start(port: Int) = {
  var serverSocket = new ServerSocket(port)
  // this .accept() method is key
  var clientSocket = serverSocket.accept()
  var out = new PrintWriter(clientSocket.getOutputStream(), true)
  var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
  // var greeting: String = in.readLine()
  // println(greeting)
  // if ("hello server".equals(greeting)) {
  //   out.println("hello client")
  // } else {
  //   out.println("unrecognised greeting")
  // }
  // println("runndijfgniajsnfiawhat")

  while (true) {
    var randomnum = new Random()
    var randomnumstring = randomnum.toString()
    println("looping... " + randomnumstring)
    
    // in.lines()
    // in.readLine()
    
    out.println("XPHEOXXNAJSNAINSDI")
    Thread.sleep(300)
  }
  // }

  def stop(): Unit = {
    in.close()
    out.close()
    clientSocket.close()
    serverSocket.close()
  }

  // def main(args: Array[String]): Unit = {
  //     var server: SocketServerHandler = new SocketServerHandler(6666)
  //     server
  // }
}
