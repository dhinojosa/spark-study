package com.xyzcorp

import java.lang

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
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

  test("""Case 1: Show will show a minimal amount
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
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    val schema = dataFrame.schema

    println(schema) //Show the schema

    dataFrame.show() //Show the data
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

    frame.sort("high").show()
  }

  test(
    """Case 6: Provide the solution to sort by the high column again, but
      |this time add the method explain(true) at the end of the chain, true
      |shows the full mapping of how the data will be analyzed""".stripMargin) {

    val url = this.getClass.getResource("/goog.csv")
    val frame: DataFrame = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

    println(frame.sort("High").show())
  }

  test("""Case 7: Don't like the schema? Make your own!""") {

    //Date,Open,High,Low,Close,Volume

    val schema = StructType(Array(
      StructField("Trade Date", StringType, nullable = false),
      StructField("Open", DoubleType, nullable = false),
      StructField("High", DoubleType, nullable = false),
      StructField("Low", DoubleType, nullable = false),
      StructField("Closing", DoubleType, nullable = false),
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

  test("""Case 8: We can also apply some metadata to the schema""") {
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
    """Case 9: Columns represent columns and can be used for querying
       There are four different formats in Scala to represent a column.
       These come from the org.apache.spark.sql package""") {
    import org.apache.spark.sql.functions._
    col("someColumnName")
    column("someColumnName")
    $"someColumnName" //Part of implicits
    'someColumnName //Scala Symbol
  }

  test("""Case 10: A Column can be resolved from a DataFrame""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val column = dataFrame.col("Open")
  }

  test("""Case 11: A list of columns can be resolved from a DataFrame""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    println(dataFrame.columns.toList)
  }

  test(
    """Case 12: Expressions can be used to query a DataFrame and has a very
      | similar flavor to SQL, in fact it is the driving engine to it.""") {
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)

    import org.apache.spark.sql.functions._
    val column: Column = expr("Volume > 2000000")

    dataFrame.where(column).show()
  }

  test(
    """Case 13: Create a query that will show days where the closing price of
      | Google's stock is less than the opening price that trading day using
      | an expression. Remove pending when ready""") {
    import org.apache.spark.sql.functions._

    val url = this.getClass.getResource("/goog.json")

    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val column: Column = expr("Close > Open")

    println(dataFrame.where(column).show())

    pending
  }

  test(
    """Case 14: Create a query that will show days where the closing price of
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
    """Case 15: Creating a manual row so as it convert it into dataframe
      |or add it to an existing one""") {
    val newRow = Row("24-Jul-17", 967.84, 967.84, 960.33, 961.08, 1493955)
    println(newRow)
  }

  test(
    """Case 16: Make your own DataFrame using custom Rows.
      | We will also use
      | parallelize which will create a construct called RDD""") {

    val employeeSchema = new StructType(Array(
      StructField("firstName", StringType, nullable = false),
      StructField("middleName", StringType, nullable = true),
      StructField("lastName", StringType, nullable = false),
      StructField("salaryPerYear", IntegerType, nullable = false)
    ))

    val employees =
      List(Row("Abe", null, "Lincoln", 40000),
        Row("Martin", "Luther", "King", 80000),
        Row("Ben", null, "Franklin", 82000),
        Row("Toni", null, "Morrisson", 82000))

    val rdd: RDD[Row] = sparkContext.parallelize(employees) //uses an rdd more on that
    val dataFrame = sparkSession.createDataFrame(rdd, employeeSchema)

    val value = dataFrame.where("length (firstName) == 3")

    println(value.show())
  }

  test(
    """Case 17: Select will select columns from the dataFrame, in this
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

  test("""Case 18: Select can use any manifestation of columns""") {
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

  test("""Case 19: Select and expr is so common there is selectExpr""") {
    import org.apache.spark.sql.functions._
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val result = dataFrame.selectExpr("DATE as TRADEDATE", "DATE")
    result.show()
  }

  test("""Case 20: Showing all columns while offering an extra.""") {
    import org.apache.spark.sql.functions._
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val result = dataFrame.selectExpr("*", "DATE as TRADEDATE")
    result.show()
  }

  test(
    """Case 21: You can also include a constant within a dataFrame
      |using CONSTANT""") {
    import org.apache.spark.sql.functions._
    val url = this.getClass.getResource("/goog.json")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)
    val result = dataFrame.select(expr("*"), lit(30).as("CONSTANT"))
    result.show()
  }

  test(
    """Case 22: Joining two sets of data. In this case let's say we
      |only need Spain""".stripMargin) {

    val countriesMedalCountDF =
      Seq(("United States", "100m Freestyle", 1, 0, 3),
        ("Spain", "100m Butterfly", 2, 1, 1),
        ("Japan", "100m Butterfly", 0, 3, 0),
        ("Spain", "100m Freestyle", 0, 0, 3),
        ("Uruguay", "100m Breaststroke", 0, 1, 0),
        ("United States", "100m Breaststroke", 2, 2, 0))
        .toDF("Country", "Event", "Gold", "Silver", "Bronze")

    val countriesMedalCountDF2 =
      Seq(("United States", "100m Freestyle", 1, 0, 3),
        ("Spain", "100m Backstroke", 2, 1, 1),
        ("Spain", "200m Breaststroke", 1, 0, 0),
        ("Spain", "500m Freestyle", 3, 0, 0),
        ("Spain", "1000m Freestyle", 2, 1, 0),
        ("United States", "100m Breaststroke", 2, 2, 0))
        .toDF("Country", "Event", "Gold", "Silver", "Bronze")

    val union = countriesMedalCountDF
      .union(
        countriesMedalCountDF2.where("country == 'Spain'"))
    union.show()
  }

  test(
    """Case 23: Repartitioning from a DataFrame. This will attempt to
      |distribute your data across multiple partitions across multiple
      |machines.  This can be called with a number or by column""") {

    val largeRange = sparkSession.range(1, 1000000).toDF
    println(largeRange.rdd.getNumPartitions)

    Thread.sleep(1000)

    val largeRangeDistributed = largeRange.repartition(10) //Force it to 10, or
    // at least
    // attempt it

    println(largeRangeDistributed.rdd.getNumPartitions)
  }

  test(
    """Case 24: Coalesce is the opposite of repartition and will attempt
      |to bring it down to a certain of partitions but may often times be
      |overriden""".stripMargin) {

    val range: Dataset[lang.Long] = sparkSession.range(1, 1000000)
    val largeRange = range.toDF
    println(largeRange.rdd.getNumPartitions)

    Thread.sleep(1000)

    val largeRangeDistributed = largeRange.repartition(10)

    //Force it to 10, or
    // at least
    // attempt it

    Thread.sleep(1000)

    val coalesced = largeRangeDistributed.coalesce(5)

    println(largeRangeDistributed.rdd.getNumPartitions) //May not be 5
  }

  test(
    """Case 25: DataFrames API are powerful enough to create UDF, user
      |defined functions""".stripMargin) {

    import org.apache.spark.sql.functions._

    def is_odd(x: Int): Boolean = x % 2 != 0

    val is_odd_udf = udf(is_odd(_: Int): Boolean)

    val url = this.getClass.getResource("/goog.json")

    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .json(url.getFile)

    dataFrame.withColumn("IS_ODD_VOLUME", is_odd_udf('VOLUME)).show(5)
  }

  val cities: DataFrame = Seq(
    (1, "San Francisco", "CA"),
    (2, "Dallas", "TX"),
    (3, "Pittsburgh", "PA"),
    (4, "Buffalo", "NY"),
    (5, "Oklahoma City", "OK"),
    (6, "New York City", "NY"),
    (7, "Los Angeles", "CA"),
    (8, "Omaha", "NE")).toDF("id", "city", "state")

  val teams: DataFrame = Seq(
    (1, 7, "Rams", "Football"),
    (2, 7, "Dodgers", "Baseball"),
    (3, 6, "Giants", "Football"),
    (4, 1, "Giants", "Baseball"),
    (5, 4, "Bills", "Football"),
    (6, 3, "Pirates", "Baseball"),
    (7, 1, "49ers", "Football"),
    (8, 3, "Steelers", "Football")).toDF("id", "city_id", "team", "sport_type")


  test("Case 26: Inner joins in using Spark Dataframes") {
    val innerjoin = cities
      .join(teams, cities.col("id") === teams.col("city_id"))
    innerjoin.show()
  }

  test("Case 27: Outer joins in using Spark Dataframes") {
    val outerjoin = cities.join(teams, cities.col("id") === teams.col
    ("city_id"), "outer")
    outerjoin.show()
  }

  test("Case 28: Joining while getting rid of duplicate keys") {
    val outerjoin = cities.join(
      teams.withColumnRenamed("id", "team_id"),
      cities.col("id") === teams.col("city_id"), "outer")
      .withColumnRenamed("id", "city_id")
    outerjoin.show()
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
