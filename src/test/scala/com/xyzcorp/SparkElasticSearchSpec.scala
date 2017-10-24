package com.xyzcorp

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn


class SparkElasticSearchSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private lazy val sparkConf = new SparkConf().setAppName("spark_basic_rdd").setMaster("local[*]")
    .set("es.index.auto.create", "true")
    .set("es.nodes", "54.212.31.19")
    .set("es.port", "9200")
    .set("es.net.http.auth.user", "user")
    .set("es.nodes.wan.only", "true")
    .set("es.http.timeout", "5000")
    .set("es.net.http.auth.pass", "")


  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("ERROR")

  test("Case 1: Saving to elastic search using Map, " +
       "Elastic Search requires that elasticsearch-hadoop be downloaded") {
    import org.elasticsearch.spark._

    val content = Map("title" -> "Using Elasticsearch with Spark",
      "author" -> "Coin Bank",
      "content" -> "Using Elastic Search is a fine deal")

    sparkContext.makeRDD(Seq(content)).saveToEs("blogs/currentevents")

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
