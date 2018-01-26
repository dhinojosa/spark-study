package com.xyzcorp

import org.apache.spark.sql.SparkSession

object SparkPi {
  def main(args: Array[String]) {
    val sparkSession = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()

    val result = sparkSession
      .sparkContext
      .parallelize(1 to 10000)
      .map(x => x + 2)
      .reduce(_ + _)
    //    val sparkContext = sparkSession.sparkContext
    //    val slices = if (args.length > 0) args(0).toInt else 2
    //    val n = math.min(100000L * slices, Int.MaxValue).toInt
    //    // avoid overflow
    //    val count = sparkContext.parallelize(1 until n, slices).map { i =>
    //      val x = random * 2 - 1
    //      val y = random * 2 - 1
    //      if (x * x + y * y <= 1) 1 else 0
    //    }.reduce(_ + _)
    //    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    //    Thread.sleep(3000)
    //    sparkContext.stop()
    println(result)
    sparkSession.close()
  }
}

//Source: https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala



