package com.xyzcorp

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSparkSessionContext extends BeforeAndAfterAll {
  this: Suite =>
  val logger: Logger = Logger[LocalSparkSessionContext]
  var session: SparkSession = _

  override def beforeAll() {
    val builder = SparkSession.builder().appName("sample").master("local[2]").config("spark.executor.memory", "1g")
    session = builder.getOrCreate()
    logger.debug(s"Spark Session Created: $session")
    super.beforeAll()
  }

  override def afterAll() {
    logger.debug(s"Spark Session Closing: $session")
    session.close()
    super.afterAll()
  }
}

trait CassandraSparkSessionContext extends BeforeAndAfterAll {
  this: Suite =>
  val logger: Logger = Logger[CassandraSparkSessionContext]
  var session: SparkSession = _

  override def beforeAll() {
    val builder = SparkSession.builder()
      .appName("sample")
      .master("local[2]")
      .config("spark.cassandra.connection.host", "cas2")
    val session = builder.getOrCreate()
    logger.debug(s"Cassandra Spark Session Created: $session")
    super.beforeAll()
  }

  override def afterAll() {
    logger.debug(s"Cassandra Spark Session Closing: $session")
    session.close()
    super.afterAll()
  }
}