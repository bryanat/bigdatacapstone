package socketpack

import java.net._
import java.io._

class SocketClientHandler(ip: String, port: Int) {

  // define global class types
  // var clientSocket: Socket
  // var out: PrintWriter
  // var in: BufferedReader

  // def startConnection(ip: String, port: Int) {
  var clientSocket = new Socket(ip, port)
  var out = new PrintWriter(clientSocket.getOutputStream(), true)
  var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
  // }

  def sendMessage(msg: String): Unit = {
    out.println(msg)
  }

  def stopConnection(): Unit = {
    in.close()
    out.close()
    clientSocket.close()
  }
}
