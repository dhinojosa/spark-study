package com.xyzcorp

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object SparkStructuredStreamingWithWindows extends App {

  private lazy val sparkConf = new SparkConf()
    .setAppName("structured_stream_with_windows")
    .setMaster("local[*]")

  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  private val stream = sparkSession
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "10150")
    .option("includeTimestamp", value = true)
    .load()

  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO")

  import sparkSession.implicits._

  val words: DataFrame = stream.as[(String, Timestamp)]
    .flatMap(tp => tp._1.split(" ").map(word => (word, tp._2))
  ).toDF("word", "timestamp")


  val windowedCounts = words.groupBy(
    window($"timestamp", "5 minutes", "1 minute"), $"word"
  ).count().orderBy("window")

  val query = windowedCounts.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .option("truncate", "false")
    .start()

  query.awaitTermination()
}
