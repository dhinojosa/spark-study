package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class DecisionTreeSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_decision_tree")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext //required for conversions

  sparkContext.setLogLevel("INFO") //required for conversions

  // Heart Disease Data
  // https://www.kaggle.com/ronitf/heart-disease-uci/version/1
  //
  //  age - age in years
  //  sex - (1 = male; 0 = female)
  //  cp - chest pain type
  //  trestbpsresting - blood pressure (in mm Hg on admission to the hospital)
  //  cholserum - cholestoral in mg/dl
  //  fbs(fasting blood sugar > 120 mg/dl) (1 = true; 0 = false)
  //  restecgresting - electrocardiographic results
  //  thalachmaximum heart rate achieved
  //  exangexercise induced angina (1 = yes; 0 = no)
  //  oldpeak - ST depression induced by exercise relative to rest
  //  slope - the slope of the peak exercise ST segment
  //  ca - number of major vessels (0-3) colored by flourosopy
  //  thal - 3 = normal; 6 = fixed defect; 7 = reversable defect
  //  target - 1 or 0


  test("Case 1: Read the data in, in this case heart.csv ") {
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
      .setInputCols(Array("age", "sex", "cp", "trestbps", "chol",
        "fbs", "restecg", "thalach", "exang", "oldpeak", "slope", "ca", "thal"))
      .setOutputCol("features")

    val newFrame = assembler.transform(frame)

    newFrame.show()

    val splitData: Array[Dataset[Row]] = newFrame.randomSplit(Array(0.7, 0.3), seed = 1234L)

    val trainingData = splitData(0)
    val testingData = splitData(1)

    println("trainingData")
    println("-------------")

    trainingData.show()

    import org.apache.spark.ml.classification.DecisionTreeClassifier

    val decisionTreeClassifier = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("target")

    val model = decisionTreeClassifier.fit(trainingData)
    val result = model.transform(testingData)

    result.show()
    result.printSchema()

    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("target")
      .setPredictionCol("prediction")

    val accuracyMetric = evaluator.setMetricName("accuracy")
    val f1Metric = evaluator.setMetricName("f1")
    val accuracy = accuracyMetric.evaluate(result)
    val f1 = f1Metric.evaluate(result)
    println(s"Test set accuracy = $accuracy")
    println(s"Test set f1 = $f1")
  }
}
