package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App {

  //run with nc -lk 10150

  val conf: SparkConf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
  val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(1)) //Seconds comes from streaming

  streamingContext.sparkContext.setLogLevel("INFO")

  val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("127.0.0.1", 10150)
  val words: DStream[String] = lines.flatMap(_.split(" "))
  val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
  val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

  wordCounts.print()

  streamingContext.start()
  streamingContext.awaitTermination()
}
