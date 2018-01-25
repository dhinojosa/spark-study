package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamFactory {
  def apply: StreamingContext = {
    val conf: SparkConf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(1))
    streamingContext.sparkContext.setLogLevel("INFO")
    streamingContext.checkpoint("/tmp/checkpoints") // set checkpoint directory
    val lines = streamingContext.socketTextStream("localhost", 10150)

    //produce information over the last 30 seconds of data, every 10 seconds
    val windowedStream = lines.window(Seconds(30), Seconds(10)).checkpoint(Seconds(1))
    windowedStream.print()
    streamingContext
  }
}

object SparkStreamingWithCheckpoints extends App {

  //run with nc -lk 9090

  private val streamingContext: StreamingContext =
    StreamingContext.getOrCreate("/tmp/checkpoints", () => StreamFactory.apply)
  streamingContext.start()
  streamingContext.awaitTermination()
}
