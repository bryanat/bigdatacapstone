package kafkapack
import scala.io._
import scala.io.Source
import scala.util.matching.Regex
import scala.util.matching.UnanchoredRegex

object RegexTest extends App {
    
  val regexParseColumns = List("order_id", "customer_id",
    "customer_name", "country", "city",
    "datetime", "order_id", "product_id",
    "product_name", "product_category", "datetime",
    "order_id", "payment_type", "payment_txn_id",
    "payment_txn_success", "price", "qty",
    "failure_reason", "datetime")

  // val captialCaseReg = "([A-Z][a-z]+)".r

  /* Objective:
   * take each CSV and run it through Scala regex to clean out the bad data.
   * need to write function that either deletes or refuses bad data while letting the administrator know
   * 1. create regex variables for ease of implementation - semi-done / don't know if i need more yet
   * 2. for loop to split data into lists - done
   * 3. implement regex cleaner function thru each list
   *    - approach 1: foreach line do a match-case
   * 4. conditional to let admin know bad data was found and the lines have been taken out
   * 5. delete bad data lines from rdd/df
   * 6. final function/variable to make data queriable
   */

  val regexComma = "(?!,)[^,]*".r // split by comma
  val regexCapitalCase = "([A-Z][a-z]+)".r //Check for Capital Case and only alphabet letters
  val regexSymbols = "([^A-Z][^a-z]+)".r //Check for symbols

  lazy val citylist = "dataset-online/citylist.csv"
//   lazy val components = "dataset-online/components.csv"
  lazy val custIdNames = "dataset-online/custIdNames.csv"
  lazy val productdata = "dataset-online/productdata.csv"
  lazy val websites = "dataset-online/websites(500).csv"

  //trial with abstract and case classes
  //for match-case
  abstract class RegexIdentifier
  case class DataList(List :String)

  
  for (line <- Source.fromFile("UTF-8", citylist).getLines) {
    /*
     * ListOps in constant O(1) time, .prepend (set) is constant, head and tail (get) is constant
     */

    val listOfValues = regexComma.findAllMatchIn(line).toList

    /* -- made so I don't have to keep running program --
    * output listOfValues returns:
    * List(Ivanteyevka, Russia, )
    * List(Nagaoka, Japan, )
    * List(Osijek, Croatia, )
    * List(Cozumel, Mexico, )
    * List(Cukai, Malaysia, )
    * List(Mbarara, Uganda, )
    * List(Lucerne, Switzerland, ) */

//    using this output logic i need to find way to further split and separate the lines created and convert to string

//    val stringConvert = listOfValues.toString() //possibly used to convert list subsections to string -- pending use

    // if (!= "([A-Z][a-z]+)".r){

    // }
    // println(regexSymbols.findAllIn(listOfValues.toString()))
    println(listOfValues)


  } //end for loop citylist
  
  /*
  for (line <- Source.fromFile(components).getLines) {

    val listOfValues = regexComma.findAllMatchIn(line).toList

    // if (!= "([A-Z][a-z]+)".r){

    // }
//    println(listOfValues)

  } //end for loop components
}
*/
//List for regex - has all of the data columns for regex to refer to
//purpose for this is to ennsure match of columns



//addition of columns to filter out bad data
//Regex for Uppercase-Lowercase Letters ([A-Z][a-z]+) Capital Case
// chop up line get every character between column - every 7th column
// assume start from string
// line -> trim
// val captialCaseReg = "([A-Z][a-z]+)".r

//  start regex - working template
/*
var filename = "dataset-online\\citylist.csv"

for (line <- Source.fromFile(filename).getLines) {
      /*
       * EACH LOOP IS A LINE IN THE IMPORTED FILE
       */
      val regexComma = "(?!,)[^,]*".r // split by comma

      /*
       * ListOps in constant O(1) time, .prepend (set) is constant, head and tail (get) is constant
       */
      val listOfValues = regexComma.findAllMatchIn(line).toList

      // if (!= "([A-Z][a-z]+)".r){

      // }
    }
*/
}