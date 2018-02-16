package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkStreamingSession extends App {

  //run with nc -lk 10150

  private lazy val sparkConf = new SparkConf()
    .setAppName("stream_1")
    .setMaster("local[*]")

  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  private val lines = sparkSession
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "10150")
    .load()

  import sparkSession.implicits._

  val words =
    lines.as[String].flatMap(_.split(" "))
  val pairs =
    words.map(word => (word, 1))
  val wordCounts =
    pairs.groupBy("value").count()

  val query = wordCounts.writeStream.format("console").start()

  query.awaitTermination()
}
