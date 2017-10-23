
name := "spark-training"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.11"

resolvers += "Conjars" at "http://conjars.org/repo"

libraryDependencies ++= Seq(

  //Logging
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",

  //Testing
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",

  //Spark Core
  "org.apache.spark" %% "spark-core" % "2.2.0",

  //Spark SQL
  "org.apache.spark" %% "spark-sql" % "2.2.0",

  //Spark Streaming
  "org.apache.spark" %% "spark-streaming" % "2.2.0",

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
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
)

