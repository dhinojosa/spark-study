package com.xyzcorp

import org.apache.spark._
import org.apache.spark.streaming._
import org.scalatest.{FunSuite, Matchers}


class SparkHDFSSpec extends FunSuite with Matchers {
  test("Case 1: Run Streaming off of a file system") {

//    Logger.getLogger(classOf[SparkStreamingSpec]).getLevel
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)

    val userDirectory = System.getProperty("user.home")

    val conf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1)) //Seconds comes from streaming

    val stream = ssc.socketTextStream("localhost", 9090)
    stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
