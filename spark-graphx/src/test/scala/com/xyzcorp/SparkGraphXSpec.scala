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

  import sparkSession.implicits._ //required for conversions

  sparkContext.setLogLevel("INFO") //logging

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

  test("""Case 1: Using the relationships above we will
      | establish a graph and show the vertices""") {

    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //Show all vertices
    println("---vertices----")
    graph.vertices.collect.foreach(println)
  }

  test("""Case 2: Using the relationships above we will
         | establish a graph and show the edges""") {

    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //Show all edges
    println("---edges----")
    graph.edges.collect().foreach(println)
  }

  test("""Case 3: Using the relationships above we will
         | establish a graph and show the relationships, also known as the
         | triplets (vertex, edge, vertex). srcAttr is the source attribute,
         | attr is the relationship attribute, and dstAttr is the destination
         | attribute.""") {

    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //Show all the triplets, and then map
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
  }

  test("""Case 4: Determining a new subgraph, those with a missing relationship
        with only appropriate vertices""") {

    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //vpred is the vertice predicate, given a vertice type determine which
    // you do not want involved.
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
  }

  test(
    """Case 5: Create another graph and just show the valid
       relationships""") {

    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //vpred is the vertice predicate, given a vertice type determine which
    // you do not want involved.

    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")

    //This will show all the valid relationships
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
  }

  test("""Case 6: Running followers and users and determine the weight
    between each of the followers using a Page Rank""") {
    pending
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
