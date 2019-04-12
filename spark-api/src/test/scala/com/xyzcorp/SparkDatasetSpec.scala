package com.xyzcorp

import java.lang
import java.net.URL
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

case class Trade(date: String, open: Double,
                 high: Double,
                 low: Double,
                 close: Double,
                 volume: Long) {
  private val formatter = DateTimeFormatter.ofPattern("d-MMM-yy")
  def getLocalDate: LocalDate = LocalDate.parse(date, formatter)
}

class SparkDatasetSpec extends FunSuite with Matchers with BeforeAndAfterAll {


  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_basic_dataset")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession.builder().config(sparkConf)
    .getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO")
  lazy val url: URL = getClass.getResource("/goog.csv")

  test("""Case 1: Create a DataSet from an RDD that was created
      |  using parallelize""".stripMargin) {
    import sparkSession.implicits._
    val frame: DataFrame = sparkSession.read.csv(url.getFile)
    val dataset: Dataset[Int] = sparkSession
      .sparkContext
      .parallelize(1 to 1000).toDS()
  }

  test("Case 2: Datasets can be created from a Seq") {
    import sparkSession.implicits._
    val dataset: Dataset[String] = sparkSession
      .createDataset(List("One", "Two", "Three"))
    dataset.map(s => s.length).foreach(s => println(s))
  }

  test("""Case 3: Dataset can be created using a range and also
      |  explained before run""".stripMargin) {
    val value: Dataset[lang.Long] = sparkSession.range(1, 100)
    value.filter(x => x % 2 == 0).explain(true)
  }

  test("Case 4: Dataset can also be shown") {
    sparkSession.read.option("header", "true").csv(url.getFile).show()
  }

  test("""Case 5: Dataset can also be read from a file and
      |  have a schema inferred. Keep in mind that a DataFrame is
      |  really just a Dataset[Row]""".stripMargin) {
    val frame = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
  }

  test("Case 6: Dataset can have a case class used in its place") {
    import sparkSession.implicits._
    val dataFrame: DataFrame = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
    val items: Dataset[Trade] = dataFrame
      .as[Trade]
    items
      .filter(t => t.close > t.open)
      .foreach(t => println(t))
  }

  test("Case 7: Dataset can be expressed with a column") {
    import sparkSession.implicits._
    val items: Dataset[Row] = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
    val date: ColumnName = $"Date"
    items
      .filter($"Date".endsWith("16"))
      .sort(date.asc)
      .show()
  }

  test("Case 8: Dataset can joined with a union") {
    import sparkSession.implicits._
    val items: Dataset[Row] = sparkSession.read.option("header", "true")
      .option("inferSchema", "true").csv(url.getFile)
    val date: ColumnName = $"Date"
    items.filter($"Date".endsWith("16")).sort(date.asc).show()
  }

  test("Case 9: Take will take the first rows of data and convert them into " +
    "an Array") {
    val url = getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
    frame
      .take(5)
      .foreach(println)
  }

  test("Case 10: Converting a row to a case class") {
    val url = getClass.getResource("/goog.csv")
    import sparkSession.implicits._
    val stockTransactions: Dataset[Trade] = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
      .as[Trade]

    stockTransactions.show()
  }

  test("Case 11: Converting a row to a case class") {
    val url = getClass.getResource("/goog.csv")
    import sparkSession.implicits._
    val stockTransactions = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
      .as[Trade]
    stockTransactions
      .filter(_.getLocalDate.getYear == 2017)
      .foreach(st => println(st.volume))
  }

  test(
    """Case 12: groupByKey can group by a month, careful that we can only
       |  operate on values that are primitives
       |  or case classes""".stripMargin) {

    val url = getClass.getResource("/goog.csv")

    import sparkSession.implicits._

    val stockTransactions = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
      .as[Trade]

    import java.time.Month

    import org.apache.spark.sql.functions._

    val convertToMonthUDF = udf((x:Int) => Month.of(x).toString)

    stockTransactions
      .groupByKey(_.getLocalDate.getMonth.toString)
      .count()
      .withColumnRenamed("count(1)", "count")
      .withColumnRenamed("value", "month")
      .show(12)
  }

  override protected def beforeAll(): Unit = {
    println("Setting up the spark context")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    println("Press any key to terminate")
    StdIn.readLine()
    sparkSession.close()
    super.afterAll()
  }
}
