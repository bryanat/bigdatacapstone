package producerpack

object MainProducer {

    def startMainProducer(): Unit = {
        println("Main Producer started...")
        var trend1Vector = trend1.getTrend1()
        trend1Vector.foreach(println)
    }
}