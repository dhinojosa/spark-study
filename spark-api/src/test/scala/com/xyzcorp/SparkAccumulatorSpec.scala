package com.xyzcorp

import java.time.{LocalDate, Month}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

@SerialVersionUID(4010239L)
class SparkAccumulatorSpec
  extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_accumulators")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  private lazy val sparkContext =
    sparkSession
      .sparkContext

  sparkContext.setLogLevel("INFO")

  import sparkSession.implicits._ //required for conversions

  test(
    """Case 1: A summation or any accumulation, which is associative and
      | commutative, can be performed as a side effect by using an
      | accumulator. In this case we will create an unnamed accumulator"""
      .stripMargin) {
    val counter = new LongAccumulator
    sparkContext.register(counter)


    def februaryAccumulator(t: (String, Long)): Long = {
      import java.time.format.DateTimeFormatter
      val formatter = DateTimeFormatter.ofPattern("dd-MMM-yy")
      val date = LocalDate.parse(t._1, formatter)
      if (date.getMonth == Month.FEBRUARY) t._2
      else 0
    }

    val url = getClass.getResource("/goog.csv")
    val dataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(url.getFile)

//    val result = dataFrame
//      .mapPartitions{ row =>
//        counter.add(februaryAccumulator(row.getString(0), row.getInt(5)))
//        row.getInt(5)
//      }.reduce(_ + _)
//
//    println("Result:" + result)
//    println("Accumulator:" + counter.value)
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
