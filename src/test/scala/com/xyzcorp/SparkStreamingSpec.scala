package com.xyzcorp

import org.apache.spark._
import org.apache.spark.streaming._
import org.scalatest.{FunSuite, Matchers}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream

import scala.io.StdIn


class SparkStreamingSpec extends FunSuite with Matchers {
  test("Case 1: Run Streaming off of a file system") {

    val userDirectory = System.getProperty("user.home")

    val conf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1)) //Seconds comes from streaming

    val stream = ssc.socketTextStream("localhost", 9090)
    stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
