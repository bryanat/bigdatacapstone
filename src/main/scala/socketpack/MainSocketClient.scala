package socketpack

import scala.io.StdIn.readLine

object MainSocketClient {
  def main(args: Array[String]): Unit = {
    println("MainSocketClient is running...")

    var client: SocketClientHandler = new SocketClientHandler("localhost", 6666);
    var response: String = client.sendMessage("hello server");


    def cliInputToSocket() {
      var cliMessage = readLine()
      client.sendMessage(cliMessage)
    }
    while (true) {
      cliInputToSocket()
    }

    // assertEquals("hello client", response);

  }
}
