package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkStructuredStreaming extends App {

  //run with nc -lk 10150

  private lazy val sparkConf = new SparkConf()
    .setAppName("stream_1")
    .setMaster("local[*]")

  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  private lazy val stream: DataFrame = sparkSession
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "10150")
    .load()

  private lazy val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("INFO")

  import sparkSession.implicits._

  //Dataset is structured
  val words: Dataset[String] =
    stream.as[String].flatMap(x => x.split(" "))

  val wordCounts = words.groupBy("value").count()

  //Structured
  val query = wordCounts
    .writeStream
    .outputMode(OutputMode.Complete()) //Group By Requires Complete
    .format("console")
    .start()

  query.awaitTermination()
}
