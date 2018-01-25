package com.xyzcorp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkElasticSearchSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private lazy val sparkSession = SparkSession.builder()
    .config("spark_es_rdd", "local[*]")
    .config("es.index.auto.create", "true")
    .config("es.nodes", "es0") //Place your address here
    .config("es.port", "9200")
    .config("es.net.http.auth.user", "user")
    .config("es.nodes.wan.only", "true")
    .config("es.http.timeout", "5000")
    .config("es.net.http.auth.pass", "password")
    .getOrCreate()

  private lazy val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("INFO")

  test("Case 1: Saving to elastic search using Map, " +
       "Elastic Search requires that elasticsearch-hadoop be downloaded") {
    import org.elasticsearch.spark._

    val content = Map("title" -> "I love Dallas AT&T",
      "author" -> "Danno",
      "content" -> "Using Elastic Search is a fine deal with Spark.  Rock on Forever!")

    val value1: RDD[Map[String, String]] = sparkContext.makeRDD(Seq(content))
    value1.saveToEs("blogs/currentevents")

    println("Saved!")
  }

  test("Case 2: Searching with Elastic Search") {
    import org.elasticsearch.spark._
    val rdd: RDD[(String, String)] = sparkContext.esJsonRDD("blogs/currentevents")
    rdd.foreach { t => println(t._2) }
    println("Saved!")
  }

  test("Case 3: Saving Existing JSON") {
    import org.elasticsearch.spark._
    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    sparkContext.makeRDD(Seq(json1, json2)).saveJsonToEs("/spark/json-trips")
    println("Saved!")
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
