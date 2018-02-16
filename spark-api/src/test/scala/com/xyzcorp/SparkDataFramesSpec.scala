package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn


class SparkDataFramesSpec
  extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_dataframes")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO")

  import sparkSession.implicits._ //required for conversions

  test(
    """Case 1: Show will show a minimal amount
        of data from the spark data set""") {
    val url = this.getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession
      .read
      .csv(url.getFile)
    println(frame.show())
  }

  test("""Case 2: Show the schema by calling schema""") {
    val url = this.getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession
      .read
      .csv(url.getFile)
    println(frame.schema)
  }

  test("""Case 3: Determining the schema automatically""") {
    val url = this.getClass.getResource("/goog.csv")
    val schema = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
      .schema

    println(schema)
  }

  test(
    """Case 4: Reading the Schema from JSON, something to be aware of,
          is that json will need to be a line by line JSON. You also can
          read other 'big data' formats like ORC, Parquet, etc. """) {
    val url = getClass.getResource("/goog.json")
    val schema = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
      .schema
    println(schema)
  }

  test(
    """Case 5: Using your intuition, what do you think the method will
       be to sort by the high price of the trade? Use that method, don't know
       the name of the column? Run the previous test and show
       the results using show()""".stripMargin) {

    val url = this.getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)
    pending
  }

  test("""Case 6: Don't like the schema? Make your own!""") {
    val schema = StructType(Array(
      StructField("Closing", DoubleType, nullable = false),
      StructField("Trade Date", StringType, nullable = false),
      StructField("High", DoubleType, nullable = false),
      StructField("Low", DoubleType, nullable = false),
      StructField("Open", DoubleType, nullable = false),
      StructField("Trade Volume", LongType, nullable = false))
    )
    val url = this.getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession
      .read
      .option("header", "true")
      .schema(schema)
      .csv(url.getFile)
    frame.show()
  }

  test("""Case 7: We can also apply some metadata to the schema""") {
    val schema = StructType(Array(
      StructField("Closing",
        DoubleType,
        nullable = false,
        Metadata.fromJson("{closing time: 3:00 Eastern}")),
      StructField("Trade Date", StringType, nullable = false),
      StructField("High", DoubleType, nullable = false),
      StructField("Low", DoubleType, nullable = false),
      StructField("Open", DoubleType, nullable = false),
      StructField("Trade Volume", LongType, nullable = false))
    )
    val url = this.getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession
      .read
      .option("header", "true")
      .schema(schema)
      .csv(url.getFile)
    frame.show()
  }

  test(
    """Case 8: Columns represent columns and can be used for querying
       There are four different formats in Scala to represent a column.
       These come from the org.apache.spark.sql package""") {
    import org.apache.spark.sql.functions._
    col("someColumnName")
    column("someColumnName")
    $"someColumnName" //Part of implicits
    'someColumnName //Scala Symbol
  }

  test("""Case 8: A Column can be resolved from a DataFrame""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val column = dataFrame.col("Open")
  }

  test("""Case 9: A list of columns can be resolved from a DataFrame""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    println(dataFrame.columns.toList)
  }

  test(
    """Case 10: Expressions can be used to query a DataFrame and has a very
      | similar flavor to SQL, in fact it is the driving engine to it.""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)

    import org.apache.spark.sql.functions._
    val column: Column = expr("Volume > 2000000")

    println(dataFrame.where(column).show())
  }

  test(
    """Case 11: Create a query that will show days where the closing price of
      | Google's stock is less than the opening price that trading day using
      | an expression. Remove pending when ready""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val column: Column = ???

    println(dataFrame.where(column).show())

    pending
  }

  test(
    """Case 12: Create a query that will show days where the closing price of
      | Google's stock is less than the opening price that trading day using
      | an expression. Remove pending when ready""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val column: Column = ???

    println(dataFrame.where(column).show())

    pending
  }

  test(
    """Case 13: Creating a manual row so as it convert it into dataframe
      |or add it to an existing one""") {
    val newRow = Row("24-Jul-17", 967.84, 967.84, 960.33, 961.08, 1493955)
    println(newRow)
  }

  test(
    """Case 14: Make your own DataFrame using custom Rows.
      | We will also use
      | parallelize which will create a construct called RDD""") {

    val employeeSchema = new StructType(Array(
      StructField("firstName", StringType, false),
      StructField("middleName", StringType, true),
      StructField("lastName", StringType, false),
      StructField("salaryPerYear", IntegerType, false)
    ))

    val employees =
      List(Row("Abe", null, "Lincoln", 40000),
        Row("Martin", "Luther", "King", 80000),
        Row("Ben", null, "Franklin", 82000),
        Row("Toni", null, "Morrisson", 82000))

    val rdd = sparkContext.parallelize(employees)
    val dataFrame = sparkSession.createDataFrame(rdd, employeeSchema)

    val value = dataFrame.where("length (firstName) == 3")

    println(value.show())
  }

  test(
    """Case 15: Select will select columns from the dataFrame, in this
      |example we will use it in conjunction with where""".stripMargin) {
    import org.apache.spark.sql.functions._
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val result = dataFrame
      .select(col("Date"), $"Open", column("Close"))
      .where("Volume > 2000000")
    result.show()
  }

  test("""Case 16: Select can use any manifestation of columns""") {
    import org.apache.spark.sql.functions._
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val result = dataFrame
      .select(col("Date"), $"Open", column("Close"), 'Volume)
      .where("Volume > 2000000")
    result.show()
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
