package com.xyzcorp

import java.time.{LocalDate, Month}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkBroadcastVariablesSpec
  extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_accumulators")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  private lazy val sparkContext =
    sparkSession
      .sparkContext

  sparkContext.setLogLevel("INFO") //required for conversions

  test(
    """Case 1: A summation or any accumulation, which is associative and
      | commutative, can be performed as a side effect by using an
      | accumulator. In this case we will create an unnamed accumulator"""
      .stripMargin) {
    val counter = new LongAccumulator
    sparkContext.register(counter)
    val url = getClass.getResource("/goog.csv")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
    //println("Result:" + result)
    //println("Accumulator:" + counter.value)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    println("Press any key to terminate")
    StdIn.readLine()
    sparkSession.stop()
    sparkContext.stop()
    super.afterAll()
  }
}
