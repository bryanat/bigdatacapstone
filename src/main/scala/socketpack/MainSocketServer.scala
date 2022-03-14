package socketpack

object MainSocketServer {
  def main(args: Array[String]): Unit = {
    println("MainSocketServer is running...")

    var server: SocketServerHandler = new SocketServerHandler(6666)

    // var client: SocketClientHandler = new SocketClientHandler("127.0.0.1", 6666);
    // // client.startConnection("127.0.0.1", 6666);
    // var response: String = client.sendMessage("hello server");
    // // assertEquals("hello client", response);

  }
}
