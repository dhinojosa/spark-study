package com.xyzcorp

import java.net.URL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkSQLSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf().setAppName("spark_sql").setMaster("local[*]")
  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO")

  lazy val url: URL = getClass.getResource("/goog.csv")

  test("""Case 1: Read from from a file and create a temporary view with the
      | data based on the frame""") {

    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    frame.createOrReplaceTempView("google_data")

    val frame1 = sparkSession.sql("SELECT Date, Open, Close from google_data")
    frame1.show()
    frame1.explain(true)
  }

  test("""Case 2: Read from from a file and sort the data by the date either
      |ascending or decending based on the knowledge.
      |using previous SQL Knowledge. Remove Pending when you are done""") {

    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    frame.createOrReplaceTempView("google_data")

    val result = sparkSession.sql(???)
    result.show()
    result.explain(true)

    pending
  }

  test("""Case 3: In this challenge, select the date, open, and close from
      | google_data where the close price was less than the open price and
      | sort by date in either ascending or descending. Verify the results
      | . Remove pending when you are done.""") {

    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    frame.createOrReplaceTempView("google_data")

    val result = sparkSession.sql(???)
    result.show()
    result.explain(true)

    pending
  }

  test(
    """Case 4: Tougher challenge: What is the equivalent of the above without
      |using SparkSQL and just using the DataFrame API? Refer to the
      |DataFramesSpec for more information""") {

    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    import org.apache.spark.sql.functions._

    val result:DataFrame = ???

    result.show()

    pending
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
