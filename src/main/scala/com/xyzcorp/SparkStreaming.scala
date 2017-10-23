package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App {
  //run with nc -lk 9090

  val conf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(1)) //Seconds comes from streaming

  val stream = ssc.socketTextStream("localhost", 9090)
  stream.map(s => "Gather String from the web" + s)

  ssc.start()
  ssc.awaitTermination()
}
