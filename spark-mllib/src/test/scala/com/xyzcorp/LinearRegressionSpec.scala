package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class LinearRegressionSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_prepping_data")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO") //required for conversions

  import sparkSession.implicits._ //required for conversions

  test("Case 1: Determining the covariance between features") {
    import org.apache.spark.sql.Row
    val url = this.getClass.getResource("/heart.csv")

    val frame: DataFrame = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    frame.show()

    println("Show schema")
    frame.printSchema()

    import org.apache.spark.ml.feature.VectorAssembler
    val assembler = new VectorAssembler()
      .setInputCols(Array("age"))
      .setOutputCol("features")

    val newFrame = assembler.transform(frame)

    newFrame.show()

    val splitData: Array[Dataset[Row]] = newFrame.randomSplit(Array(0.7, 0.3), seed = 1234L)

    val trainingData = splitData(0)
    val testingData = splitData(1)

    println("trainingData")
    println("-------------")

    trainingData.show()

    import org.apache.spark.ml.regression.LinearRegression
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(trainingData)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
  }
}