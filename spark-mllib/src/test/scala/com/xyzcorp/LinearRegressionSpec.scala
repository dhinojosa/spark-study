package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
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
    import org.apache.spark.ml.linalg.{Matrix, Vectors}
    import org.apache.spark.ml.stat.Correlation
    import org.apache.spark.sql.Row

    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = data.map(Tuple1.apply).toDF("features")

    df.show()

    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println("Pearson correlation matrix:\n" + coeff1.toString)

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println("Spearman correlation matrix:\n" + coeff2.toString)
  }
}