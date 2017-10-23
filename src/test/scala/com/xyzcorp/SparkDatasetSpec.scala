package com.xyzcorp

import java.net.URL

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.apache.log4j.{Level, Logger}



import scala.io.StdIn
case class Trade(Date: String, Open: Double, High: Double, Low: Double, Close:Double, Volume:Double)

class SparkDatasetSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  lazy val conf: SparkConf = new SparkConf().setAppName("streaming_1").setMaster("local[*]")
  lazy val sparkContext: SparkContext = new SparkContext(conf)
  lazy val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  lazy val url: URL = getClass.getResource("/goog.csv")

  test("Case 1: Show will show a minimal amount of data from the spark data set") {
    import session.implicits._
    val frame: DataFrame = session.read.csv(url.getFile)
    val dataset = session.sparkContext.parallelize(1 to 1000).toDS()
  }

  test("Case 2: Datasets can be created from a Seq") {
    import session.implicits._
    val dataset = session.createDataset(Seq("One", "Two", "Three"))
    dataset.foreach(s => println(s))
  }

  test("Case 3: Dataset can be explained before run") {
    session.range(1).filter(_ == 0).explain(true)
  }

  test("Case 4: Dataset can also be shown") {
    session.read.option("header", "true").csv(url.getFile).show()
  }

  test("Case 5: Dataset can also have a schema inferred") {
    session.read.option("header", "true").option("inferSchema", "true").csv(url.getFile).printSchema()
  }

  test("Case 6: Dataset can have a case class used in its place") {
    import session.implicits._
    val items: Dataset[Trade] = session.read.option("header", "true").option("inferSchema", "true").csv(url.getFile).as[Trade]
    items.filter(t=> t.Close > t.Open).foreach(t => println(t))
  }

  test("Case 7: Dataset can be expressed with a column") {
    import session.implicits._
    val items: Dataset[Row] = session.read.option("header", "true")
      .option("inferSchema", "true").csv(url.getFile)
    val date: ColumnName = $"Date"
    items.filter($"Date".endsWith("16")).sort(date.asc).show()
  }

  test("Case 8: Dataset can joined with a union") {
    import session.implicits._
    val items: Dataset[Row] = session.read.option("header", "true")
      .option("inferSchema", "true").csv(url.getFile)
    val date: ColumnName = $"Date"
    items.filter($"Date".endsWith("16")).sort(date.asc).show()
  }

  override protected def beforeAll(): Unit = {
    println("Setting up the spark context")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    println("Press any key to terminate")
    StdIn.readLine()
    session.close()
    super.afterAll()
  }
}
