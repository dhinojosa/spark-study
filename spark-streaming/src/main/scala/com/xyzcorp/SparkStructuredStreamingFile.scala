package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

object SparkStructuredStreamingFile extends App {

  //run with nc -lk 10150

  private lazy val sparkConf = new SparkConf()
    .setAppName("stream_structured_file")
    .setMaster("local[*]")

  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val schema = StructType(Array(
    StructField("Date", StringType, nullable = false),
    StructField("Open", DoubleType, nullable = false),
    StructField("High", DoubleType, nullable = false),
    StructField("Low", DoubleType, nullable = false),
    StructField("Close", DoubleType, nullable = false),
    StructField("Volume", LongType, nullable = false)))

  private val dataFrame: DataFrame = sparkSession
    .readStream
    .option("header", "true")
    .schema(schema)
    .csv("file:///Users/danno/tmp/stream-data")

  val query = dataFrame.writeStream.format("console").start()

  query.awaitTermination()
}
