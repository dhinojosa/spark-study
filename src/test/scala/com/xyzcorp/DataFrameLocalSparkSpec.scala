package com.xyzcorp

import java.net.URL

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


class DataFrameLocalSparkSpec extends FunSuite with Matchers with LocalSparkSessionContext {

  private val url: URL = getClass.getResource("/goog.csv")
  override val logger: Logger = Logger[DataFrameLocalSparkSpec]

  test("Case 1: Show will show a minimal amount of data from the spark data set") {
    val frame: DataFrame = session.read.csv(url.getFile)
    println(frame.show())
    logger.info(frame.show().toString)
  }

  test("Case 2: Take will take the first rows of data and convert them into an Array") {
    val frame: DataFrame = session.read.csv(url.getFile)
    val rows = frame.take(5)
    logger.info(rows.toList.toString)
  }

}
