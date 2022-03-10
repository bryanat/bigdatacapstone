package mlpack

import contextpack._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf


object Correlation {



    val ssql = MainContext.getSparkSession()
    import ssql.implicits._





    def pearsonCorr(starttime: String, endtime:String): Unit = {
        val rowArray = RowArray.getArray(starttime:String, endtime:String)
        //creates an empty sequence of dense vectors
        var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0))
        //add unto the sequence of dense vectors
        val x = rowArray.foreach({row=>
            //toString is called first because each is type Any, which cannot be directly converted to Double
                var temp=Array(row(0).toString.toDouble, row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble) 
                corrArray = corrArray :+ Vectors.dense(temp) 
            })
    
            val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
            val Row(coeff1: Matrix) = org.apache.spark.ml.stat.Correlation.corr(df, "features").head
            println("Pearson correlation matrix:\n" + coeff1.toString)

            //converts Matrix to Array[Array[Double]]
            val matrixRows = coeff1.rowIter.toArray.map(_.toArray)
            // val dfp = ssql.sparkContext.parallelize(matrixRows).toDF()
            // dfp.show(false)

        }


    //similar to pearson correlation, can use either
    def spearmanCorr(starttime: String, endtime:String): Unit = {
        val rowArray = RowArray.getArray(starttime:String, endtime:String)
        //creates an empty sequence of dense vectors
        var corrArray = Seq(Vectors.dense(0,0,0,0,0,0,0,0,0,0,0))
        //add unto the sequence of dense vectors
        val x = rowArray.foreach({row=>
            //toString is called first because each is type Any, which cannot be directly converted to Double
                var temp=Array(row(0).toString.toDouble, row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble, row(11).toString.toDouble) 
                corrArray = corrArray :+ Vectors.dense(temp) 
            })

            val df = corrArray.drop(1).map(Tuple1.apply).toDF("features")
            val Row(coeff2: Matrix) = org.apache.spark.ml.stat.Correlation.corr(df, "features", "spearman").head
            println("Spearman correlation matrix:\n" + coeff2.toString) 

            //converts Matrix to Array[Array[Double]]
            val matrixRows = coeff2.rowIter.toArray.map(_.toArray)
            // val dfp = ssql.sparkContext.parallelize(matrixRows).toDF()
            // dfp.show(false)
    }



  
}

