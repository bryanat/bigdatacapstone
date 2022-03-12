package producerpack

import org.apache.spark.sql.SparkSession
import java.util.Random
import java.util.Date

import java.net._
import java.io._


// Prints the trend to the socket
// Trend One will show a larger amount of online grocery orders from North America than any other country.
// Our return string will be in the following format:
// "order_id,customer_id,customer_name,product_id,product_name,product_category,payment_type,qty,price,datetime,country,city,website,pay_id,success"

object PrintTrendToSocket {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val trendTag = "TR1"
  val rs = new RandomSelections
  val dc = new DataCollection

  //Set up the data that we will be using
  var locationVector = dc.getCityCountryList(spark)
  var customerVector = dc.getCustomersList(spark)
  var failureVector = dc.getfailReasonsList(spark)
  var websiteVector = dc.getWebsiteList(spark)
  var electronicVector = dc.getElectronicsList(spark)
  val random = new Random()

  var groceryVector = dc.getGroceryList(spark)



  def createInitialTransaction(orderID: String): String={
    val initialString = orderID+ "," + rs.getRandomCustomerID(spark)+rs.getRandomProduct(spark, "Grocery")+rs.getRandomPayment(spark)+random.nextInt(25)+","+
      "10-02-2017,"+rs.getRandomLocation(spark)+rs.getRandomWebsite(spark)+"pay_id,"+"success"
    initialString
  }


  // def infinitePrint(): Unit = {
  def main(args: Array[String]): Unit = {

    // var serverSocket = new ServerSocket
    // var clientSocket = new Socket
    // var out = new PrintWriter
    // var in = new BufferedReader
        var serverSocket = new ServerSocket(6666);
        var clientSocket = serverSocket.accept();
        var out = new PrintWriter(clientSocket.getOutputStream(), true);
        //var in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        //var greeting: String = in.readLine();
        
        //printline the output from Trend1 infinitely through the out variable
        //out.println()
        

    

    // GreetServer.start(6666);

    var orderCounter = 100000
    var orderID = trendTag+orderCounter.toString
    // for (i <- 0 to 10) {

      // var server: GreetServer = new GreetServer();
        // server.start(6666);

        println("this runs")

    while (true) {


      //Tcp.Write(createInitialTransaction(orderID))
      //OutObject.out.println(createInitialTransaction(orderID))
      println(createInitialTransaction(orderID))
      
      //out.println(createInitialTransaction(orderID))


      orderCounter = orderCounter+1
      orderID = trendTag+orderCounter.toString

      }

       //       in.close();
        out.close();
        clientSocket.close();
        serverSocket.close();


    // GreetServer.stop()

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

