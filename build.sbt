val sparkVersion = "2.2.1"

lazy val root = (project in file("."))
  .settings(Seq(
    name := "spark-training"
  )).aggregate(app, api, streaming, graphx, mllib, cassandra, kafka, s3)

lazy val commonSettings = Seq(
  version := "1.0-SNAPSHOT",
  scalaVersion := "2.11.11",
  fork in run := true,
  resolvers += "Conjars" at "http://conjars.org/repo",
  libraryDependencies ++= Seq(
    //Testing
    "org.scalatest" %% "scalatest" % "3.0.4" % "test",

    //Spark Core
    //In production you will need to put this dependency as scope provided
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",

    //Spark SQL
    //In production you will need to put this dependency as scope provided
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  )
)

lazy val api = (project in file("spark-api"))
  .settings(commonSettings)

lazy val app = (project in file("spark-app"))
  .settings(commonSettings)
  .settings(Seq(
    mainClass in Compile := Some("com.xyzcorp.SparkPi"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  ))

lazy val streaming = (project in file("spark-streaming"))
  .settings(commonSettings)
  .settings(
    libraryDependencies +=
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
  )

lazy val graphx = (project in file("spark-graphx"))
  .settings(commonSettings)
  .settings(Seq(
    libraryDependencies +=
      "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  ))

lazy val mllib = (project in file("spark-mllib"))
  .settings(commonSettings)
  .settings(Seq(
    libraryDependencies +=
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
  ))

lazy val cassandra = (project in file("spark-cassandra"))
  .settings(commonSettings)
  .settings(Seq(
    libraryDependencies +=
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"
  ))

lazy val elasticsearch = (project in file("spark-elasticsearch"))
  .settings(commonSettings)
  .settings(Seq(
    libraryDependencies ++= Seq(
      //Elastic Search
      "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.6.3",

      //Dumb Dependency for Elastic Search
      //"commons-httpclient" % "commons-httpclient" % "3.1",
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"
    )
  ))

lazy val kafka = (project in file("spark-kafka"))
  .settings(commonSettings)
  .settings(Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
    )
  ))

lazy val s3 = (project in file("spark-s3"))
  .settings(commonSettings)
  .settings(Seq(
    libraryDependencies +=
      "org.apache.hadoop" % "hadoop-aws" % "2.8.1"
  ))

lazy val hdfs= (project in file("spark-hdfs"))
  .settings(commonSettings)
  .settings(Seq(
    libraryDependencies +=
      "org.apache.hadoop" % "hadoop-client" % "2.8.1"
  ))

