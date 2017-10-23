package com.xyzcorp

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkDataFramesSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf().setAppName("spark_basic_rdd").setMaster("local[*]")
  private lazy val sparkContext: SparkContext = new SparkContext(sparkConf)
  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._ //required for conversions

  test("Case 1: Show will show a minimal amount of data from the spark data set") {
    val url = getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession.read.csv(url.getFile)
    println(frame.show())
  }

  test("Case 2: Take will take the first rows of data and convert them into an Array") {
    val url = getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
    val rows = frame.take(5).foreach(row => println(row))
  }

  test("Case 3: To DataFrame can take an RDD and convert to a DataFrame") {
    val rdd = sparkContext.parallelize(1 to 100)
    val dataFrame = rdd.toDF("amounts")
    dataFrame.show(10)
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
