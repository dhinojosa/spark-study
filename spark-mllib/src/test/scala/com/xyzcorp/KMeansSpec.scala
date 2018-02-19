package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class KMeansSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_kmeans")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO") //required for conversions

  test("Case 1: Sample KMeans Step Data") {
    val data = sparkContext
      .textFile(getClass
        .getResource("/kmeans-data.txt").getPath)

    val parsedData = data.map(
      s => Vectors.dense(s.split(' ')
        .map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Save and load model
    clusters.save(sparkContext,
      "target/org/apache/spark/KMeansExample/KMeansModel")
      val sameModel = KMeansModel.load(sparkContext,
        "target/org/apache/spark/KMeansExample/KMeansModel")

    println(sameModel)
    //  // $example off$
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

