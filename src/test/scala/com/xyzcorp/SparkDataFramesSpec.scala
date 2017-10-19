package com.xyzcorp

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkDataFramesSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private var session:SparkSession = _
  private var conf:SparkConf = _

  private val logger = Logger.apply(getClass)

  test("Case 1: Show will show a minimal amount of data from the spark data set") {
    val url = getClass.getResource("/goog.csv")
    val frame: DataFrame = session.read.csv(url.getFile)
    println(frame.show())
    logger.info(frame.show().toString)
  }

  test("Case 2: Take will take the first rows of data and convert them into an Array") {
    val url = getClass.getResource("/goog.csv")
    val frame: DataFrame = session.read.csv(url.getFile)
    val rows = frame.take(5)
    logger.info(rows.toList.toString)
  }

  test("Case 3: To DataFrame can take an RDD and convert to a DataFrame") {
      pending
  }

  override protected def beforeAll(): Unit = {
    println("Setting up the spark context")
    conf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
    session = SparkSession.builder().config(conf).getOrCreate()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    println("Press any key to terminate")
    StdIn.readLine()
    super.afterAll()
  }
}
