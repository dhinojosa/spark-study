package com.xyzcorp

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StructuredKafkaStreaming extends App {
  val kafkaParams: Map[String, AnyRef] = Map[String, Object](
    "bootstrap.servers" -> "kaf0:9092,kaf1:9092", //Use your own addresses
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka_spark", //Create your own
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val conf: SparkConf = new SparkConf()
    .setAppName("kafka_streaming")
    .setMaster("local[*]")

  private lazy val sparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val df = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kaf0:9092,kaf1:9092")
    .option("subscribe", "scaled-cities")
    .load()

  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]


}