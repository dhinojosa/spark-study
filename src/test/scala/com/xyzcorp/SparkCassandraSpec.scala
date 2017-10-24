package com.xyzcorp

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkCassandraSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private lazy val sparkConf = new SparkConf().setAppName("spark_cassandra_rdd")
    .set("spark.cassandra.connection.host", "34.235.137.174")
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "")
    .setMaster("local[*]")

  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("ERROR")

  test("Case 1: Connecting to a Cassandra host using SparkConf") {
    val rdd = sparkContext.cassandraTable("music", "artist")
    rdd.foreach(println)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    println("Press enter to terminate")
    StdIn.readLine()
    sparkSession.stop()
    sparkContext.stop()
    super.afterAll()
  }
}
