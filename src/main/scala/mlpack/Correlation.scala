package mlpack

import contextpack._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.sql.Row

object Correlation {



    val ssql = MainContext.getSparkSession()
    import ssql.implicits._


    //dataframe would continuouly updated, but correlation matrix will be on static data
    //could show correlation at different timess to client for verification purposes

    def getArray(): Array[Row] = {
        //convert to an array of rows
        val rowArray = ssql.sql("SELECT product_id, qty, price, datetime, payment_txn_sucess FROM csmessages_hive_table").toDF("product_id", "qty", "price", "datetime", "payment_txn_sucess").collect()
        rowArray
    }



    def pearsonCorr(time: String): Unit = {
        val rowArray = getArray()
        var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0))
        val x = rowArray.foreach({row=>
                var temp=Array(row(0).toString.toDouble, row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble) 
                corrArray = corrArray :+ Vectors.dense(temp) 
            })
    
            val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
            val Row(coeff1: Matrix) = org.apache.spark.ml.stat.Correlation.corr(df, "features").head
            //println("Pearson correlation matrix:\n" + coeff1.toString)
            val matrixRows = coeff1.rowIter.toArray.map(_.toArray)
            val dfp = ssql.sparkContext.parallelize(matrixRows).toDF()
            dfp.show(false)
    }



  
}

