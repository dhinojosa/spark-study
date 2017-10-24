package com.xyzcorp

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkDataFramesSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf().setAppName("spark_dataframes").setMaster("local[*]")
  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("ERROR")
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
    frame.take(5).foreach(row => println(row))
  }

  test("Case 3: To DataFrame can take an RDD and convert to a DataFrame") {
    val rdd = sparkContext.parallelize(1 to 100)
    val dataFrame = rdd.toDF("amounts")
    val value1: Dataset[Int] = dataFrame.map(row => row.getInt(0))
  }

  test("Case 4: Show will show a minimal amount of data from the spark data set") {
    val url = getClass.getResource("/goog.json")
    val frame: DataFrame = sparkSession.read.csv(url.getFile)
    frame.filter($"_corrupt_record").show()
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
