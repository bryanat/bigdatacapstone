package kafkapack

import contextpack._

object StructWrite {


    def send(): Unit={


        val sc = MainContext.getSparkSession()
        import sc.implicits._
        var df = sc.read.csv("dataset-online/productdata.csv")
        val ds = df
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
        .option("topic", "topic1")
        .start()

    }



}
