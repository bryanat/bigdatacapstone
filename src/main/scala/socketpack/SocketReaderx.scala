package socketpack

import java.util.Random
import java.util.Date

import java.net._
import java.io._

// Trend One will show a larger amount of online grocery orders from North America than any other country.
// Our return string will be in the following format:
// "order_id,customer_id,customer_name,product_id,product_name,product_category,payment_type,qty,price,datetime,country,city,website,pay_id,success"

object TrendPrintToSocket33 {

  def main(args: Array[String]): Unit = {

    ////only need to create a socket server once per port (but multiple client sockets can exist per port)
    //var serverSocket = new ServerSocket(6666);
    var clientSocket = new Socket
    var socketAddress = new InetSocketAddress("localhost", 6666)
    var clientSocketConnect = clientSocket.connect(socketAddress)
    
    //var out = new OutputStreamWriter(clientSocket.getOutputStream());
    var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    in.readLine()

    println("starting infinite loop...")
    while (true) {

      var randomnum = new Random()
      var randomnumstring = randomnum.toString()
      println("looping... " + randomnumstring )
      
      // this line below stream the strings of text you see in the console, the same strings are streamed in through the socket
      // in.readLine()

      }

      //in.close();
      //out.close();
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


