package com.xyzcorp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkSQLSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkContext:SparkContext = _

  test("Case 1: Read from from a file and read the information from the and count all the lengths") {
    val fileLocation = getClass.getResource("/goog.json").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 3)
    val lineLengths: RDD[Int] = lines.map(s => s.length)
    val totalLength: Int = lineLengths.reduce((a, b) => a + b)
    totalLength should be (25560)
  }

  test("Case 2: Parallelize will produce a stream of information across 4 partitions") {
    val paralleled: RDD[Int] = sparkContext.parallelize(1 to 10, 4)
    val result = paralleled.map(x => x + 40).collect()
    result should be (Array.apply(41, 42, 43, 44, 45, 46, 47, 48, 49, 50))
  }

  override protected def beforeAll(): Unit = {
    println("Setting up the spark context")
    val conf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
    sparkContext = new SparkContext(conf)
    super.beforeAll()
  }


  override protected def afterAll(): Unit = {
    println("Press any key to terminate")
    StdIn.readLine()
    super.afterAll()
  }
}
