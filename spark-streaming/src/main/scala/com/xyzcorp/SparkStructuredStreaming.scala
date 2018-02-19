package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkStructuredStreaming extends App {

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

  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO")

  import sparkSession.implicits._

  val words: Dataset[String] =
    lines.as[String].flatMap(_.split(" "))

  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()
}
