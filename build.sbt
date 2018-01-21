
name := "spark-training"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.11"

fork in run := true

resolvers += "Conjars" at "http://conjars.org/repo"

val sparkVersion = "2.2.1"

mainClass in Compile := Some("com.xyzcorp.SimpleSpark")

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    println(s"unknown and resolving $x")
    MergeStrategy.first
}

libraryDependencies ++= Seq(

  //Testing
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",

  //Spark Core
  //In production you will need to put this dependency as scope provided
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",

  //Spark SQL
  //In production you will need to put this dependency as scope provided
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  //Spark Streaming
  //In production you will need to put this dependency as scope provided
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

  //Spark Mllib
  //In production you will need to put this dependency as scope provided
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  //Spark Graphx
  //In production you will need to put this dependency as scope provided
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",

  //Hadoop AWS
  "org.apache.hadoop" % "hadoop-aws" % "2.8.1",

  //Hadoop Client for reading hdfs
  "org.apache.hadoop" % "hadoop-client" % "2.8.1",

  //Cassandra Connector
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3",

  //Elastic Search
  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.6.3",

  //Dumb Dependency for Elastic Search
  "commons-httpclient" % "commons-httpclient" % "3.1",

  //Kafka
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
)

