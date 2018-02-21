package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class MultivariateStatisticalSummary
  extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_multivariate_statistical_summary")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO") //required for conversions

  test("Case 1: Multivariate Statistic Summary") {
    val observations = sparkContext.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )

    val summary = Statistics.colStats(observations)
    println(summary.mean) // a dense vector containing the mean value for each column
    println(summary.variance) // column-wise variance
    println(summary.numNonzeros) //All non zeros
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    println("Press enter to terminate")
    StdIn.readLine()
    sparkSession.stop()
    sparkContext.stop()
    super.afterAll()
  }
}
