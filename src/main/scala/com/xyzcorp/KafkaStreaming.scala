package com.xyzcorp

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming extends App {
  val kafkaParams: Map[String, AnyRef] = Map[String, Object](
    "bootstrap.servers" -> "kaf0:9092,kaf1:9092", //Use your own addresses
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafka_spark", //Create your own
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val conf: SparkConf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
  val streamingContext = new StreamingContext(conf, Seconds(1)) //Seconds comes from streaming
  streamingContext.sparkContext.setLogLevel("INFO")

  val topics = Array("scaled-cities")
  val stream: InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.map(cr => "Received: " + cr.value()).foreachRDD(rdd => rdd.foreach(println))

  streamingContext.start()
  streamingContext.awaitTermination()
}
