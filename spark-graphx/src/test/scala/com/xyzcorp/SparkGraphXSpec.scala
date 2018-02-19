package com.xyzcorp

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkGraphXSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_graphx")
    .setMaster("local[*]")
  private lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO") //required for conversions


  test("Case 1: Setting up Relationships") {
    val users: RDD[(VertexId, (String, String))] =
      sparkContext.parallelize(Array(
        (3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))

    val relationships: RDD[Edge[String]] =
      sparkContext.parallelize(Array(
        Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague"))
      )

    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
  }

  test("""Case 2: Running followers and users and determine the weight
    between each of the followers""") {

    val followersPath = getClass.getResource("/followers.txt").getPath
    val usersPath = getClass.getResource("/users.txt").getPath

    val graph = GraphLoader.edgeListFile(sparkContext, followersPath)

    val ranks = graph.pageRank(0.0001).vertices

    val users = sparkContext.textFile(usersPath).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
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
