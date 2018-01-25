package com.xyzcorp

import org.apache.spark.sql.SparkSession

object SimpleSpark extends App {
  private val session = SparkSession.builder().appName("simple_spark").getOrCreate()
  private val result: Long = session.range(1, 1000).count()
  println(result)
}
