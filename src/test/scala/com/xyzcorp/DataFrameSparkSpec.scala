package com.xyzcorp

import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class CassandraSparkSpec extends FunSuite with Matchers {

  

  test("Connectability to a csv") {
    val builder = SparkSession.builder().appName("sample").master("local[2]").config("spark.executor.memory", "1g")
    val session = builder.getOrCreate()
    val url = getClass.getResource("/goog.csv")
    val frame = session.read.csv(url.getFile)
    println(frame.show())
  }

}
