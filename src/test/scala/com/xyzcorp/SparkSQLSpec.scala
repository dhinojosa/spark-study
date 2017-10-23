package com.xyzcorp

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkSQLSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf().setAppName("spark_basic_rdd").setMaster("local[*]")
  private lazy val sparkContext: SparkContext = new SparkContext(sparkConf)
  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  lazy val url: URL = getClass.getResource("/goog.csv")


  test("Case 1: Read from from a file and create a temporary view with the data") {
    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    frame.createOrReplaceTempView("google_data")

    val frame1 = sparkSession.sql("SELECT Date, Open, Close from google_data")
    frame1.show()
    frame1.explain(true)
  }

  test("Case 2: Read from from a file and sort the data") {
    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    frame.createOrReplaceTempView("google_data")

    val frame1 = sparkSession.sql("SELECT * from google_data SORT BY Date DESC")
    frame1.show()
    frame1.explain(true)
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
