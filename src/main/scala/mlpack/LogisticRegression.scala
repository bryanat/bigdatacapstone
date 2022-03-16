package mlpack
import contextpack._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.ml.param.ParamMap

object LogisticRegression {

     val ssql = MainContext.getSparkSession()
    import ssql.implicits._

    //we also can use sampled data, refer to Sampling.scala inside mlpack

    def logistic1(starttime:String, endtime:String): Unit = {


        //first we train the model using around 80 sampling data
        //refer to Samplling.scala in mlpack for different sampling techniques
        val rowArrayTrain = RowArray.getArray(starttime, endtime)
        var corrArrayTrain = Seq(Tuple2(0.toDouble, Vectors.dense(0,0,0,0,0,0,0,0,0,0)))
        rowArrayTrain.foreach({row=>
        //the first column is label, rest are features
            var temp=Array(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                    row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble) 
                corrArrayTrain = corrArrayTrain :+ Tuple2(row(0).toString.toDouble, Vectors.dense(temp))
        })
        val training = corrArrayTrain.drop(1).toDF("label", "features")
        training.show()
        // Create a LogisticRegression instance. This instance is an Estimator.
        val lr = new LogisticRegression()
        // Print out the parameters, documentation, and any default values.
        //println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

        // We may set parameters using setter methods.
        lr.setMaxIter(10)
          .setRegParam(0.01)

          // Learn a LogisticRegression model. This uses the parameters stored in lr.
        val model1 = lr.fit(training)
        //println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

        // We may alternatively specify parameters using a ParamMap,
        // which supports several methods for specifying parameters.
        val paramMap = ParamMap(lr.maxIter -> 30)
          .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
          .put(lr.regParam -> 0.1, lr.threshold -> 0.35)  // Specify multiple Params.

        // One can also combine ParamMaps.
        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
        val paramMapCombined = paramMap ++ paramMap2

        // Now learn a new model using the paramMapCombined parameters.
        // paramMapCombined overrides all parameters set earlier via lr.set* methods.
        val model2 = lr.fit(training, paramMapCombined)
        //println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)





        //Then we test using around 20% sampling data

        val rowArrayTest = RowArray.getArray(starttime, endtime)
        var corrArrayTest = Seq(Tuple2(0.toDouble, Vectors.dense(0,0,0,0,0,0,0,0,0,0)))
        rowArrayTest.foreach({row=>
        //find the fire_size_class from ID
            var temp=Array(row(1).toString.toDouble, row(2).toString.toDouble, row(3).toString.toDouble, row(4).toString.toDouble, row(5).toString.toDouble,
                        row(6).toString.toDouble, row(7).toString.toDouble, row(8).toString.toDouble, row(9).toString.toDouble, row(10).toString.toDouble) 
                    corrArrayTest = corrArrayTest :+ Tuple2(row(0).toString.toDouble, Vectors.dense(temp))
        })
        val test = corrArrayTest.drop(1).toDF("label", "features")
        model1.transform(test)
          .select("features", "label", "probability", "prediction")
          .collect()
          .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
            //println(s"($features, $label) -> prob=$prob, prediction=$prediction")
            println(s"($label) -> prediction=$prediction")
            Thread.sleep(1000)

          }

  
}

}