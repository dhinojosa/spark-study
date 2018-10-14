package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkDiscretizedStreamingWithWindows extends App {

  //run with nc -lk 10150

  val conf: SparkConf = new SparkConf()
    .setAppName("streaming_windowed")
    .setMaster("local[*]")

  val streamingContext: StreamingContext =
    new StreamingContext(conf, Seconds(1)) //Seconds comes from streaming

  streamingContext.sparkContext.setLogLevel("INFO")

  val lines: ReceiverInputDStream[String] =
    streamingContext.socketTextStream("localhost", 10150)

  // produce information over the last 30
  // seconds of data,
  // every 10 seconds
  val windowedStream: DStream[String] =
    lines
      .window(Seconds(30), Seconds(10))

  windowedStream.print()

  streamingContext.start()
  streamingContext.awaitTermination()
}
