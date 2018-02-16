package com.xyzcorp

import org.apache.spark.sql.SparkSession
import scala.math.random

object SparkPi {
  def main(args: Array[String]) {
    val sparkSession = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    // avoid overflow
    val count = sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    sparkSession.close()
  }
}

//Source: https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala



