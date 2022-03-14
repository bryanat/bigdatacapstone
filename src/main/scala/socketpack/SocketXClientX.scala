package socketpack

import java.util.Random
import java.util.Date

import java.net._
import java.io._

// Trend One will show a larger amount of online grocery orders from North America than any other country.
// Our return string will be in the following format:
// "order_id,customer_id,customer_name,product_id,product_name,product_category,payment_type,qty,price,datetime,country,city,website,pay_id,success"

object SocketXClientX {

  def main(args: Array[String]): Unit = {

    var clientSocket = new Socket
    println("A2")
    var socketAddress = new InetSocketAddress("localhost", 6666)
    println("A3")
    var clientSocketConnect = clientSocket.connect(socketAddress)

    // var out = new OutputStreamWriter(clientSocket.getOutputStream())
    // var out = new PrintWriter(clientSocket.getOutputStream())
    // var out = new PrintWriter(clientSocket.getOutputStream())

    println("A4")
    var out = new PrintWriter(clientSocket.getOutputStream(), true)
    println("A5")
    var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
    println("A6")
    var greeting = in.readLine()
    println("A7")
    println("starting if else loop...")
    if ("hello server".equals(greeting)) {
      out.println("hello client")
    } else {
      out.println("unrecognised greeting")
    }
    println("ending if else loop...")
    // var out = new BufferedWriter( OutputStreamWriter(clientSocket.getOutputStream()))
    // var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))

    println("starting infinite loop...")
    while (true) {

      var randomnum = new Random()
      var randomnumstring = randomnum.toString()
      println("looping... " + randomnumstring)

      // this line below stream the strings of text you see in the console, the same strings are streamed in through the socket
      out.println("HELLOXZOPXOZOEPOPXOPXNENX")

      Thread.sleep(300)
    }

    // in.close();
    out.close();
    clientSocket.close();

  }

  // ALL OF THE COMMENTED BELOW IS JUST FOR TESTING DIFFERENT METHODS OF DataCollection AND RandomSelections
//    val test = dc.getGroceryList(spark)
//    val test2 = dc.getSportsList(spark)
//    test.foreach(println)
//    test2.foreach(println)
//    val test3 = dc.filterByPriceAbove(spark, 500)
//    test3.foreach(println)
//    val test4 = dc.filterByPriceBelow(spark, 500)
//    test4.foreach(println)
//    println(rs.getRandomCustomerID(spark))
//    println(rs.getRandomCustomerID(spark))
//    println(rs.getRandomCustomerID(spark))
//    println(rs.getRandomWebsite(spark))
//    println(rs.getRandomWebsite(spark))
//    println(rs.getRandomWebsite(spark))
//    println(rs.getRandomProduct(spark))
//    println(rs.getRandomProduct(spark))
//    println(rs.getRandomProduct(spark))
//    println(rs.getRandomCategory(spark))
//    println(rs.getRandomCategory(spark))
//    println(rs.getRandomCategory(spark))

//    println(customerVector(49))
//    println(customerVector(49).get(1))
}
