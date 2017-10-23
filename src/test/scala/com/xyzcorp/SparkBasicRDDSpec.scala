package com.xyzcorp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkBasicRDDSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private lazy val sparkConf = new SparkConf().setAppName("spark_basic_rdd").setMaster("local[*]")
  private lazy val sparkContext: SparkContext = new SparkContext(sparkConf)
  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._ //required for conversions

  test("Case 1: Read from from a file and read the information from the and count all the lengths") {
    val fileLocation = getClass.getResource("/goog.json").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 3)
    val lineLengths: RDD[Int] = lines.map(s => s.length)
    val totalLength: Int = lineLengths.reduce((a, b) => a + b)
    totalLength should be(25560)
  }

  test("Case 2: Parallelize will produce a stream of information across 4 partitions") {
    val paralleled: RDD[Int] = sparkContext.parallelize(1 to 10, 4)
    val result = paralleled.map(x => x + 40).collect()
    result should be(Array.apply(41, 42, 43, 44, 45, 46, 47, 48, 49, 50))
  }

  test("Case 3: Distinct will retrieve all the content and show distinct items") {
    val fileLocation = getClass.getResource("/rotj.txt").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 5)
    val words: RDD[String] = lines
      .flatMap(con => con.split("""\n"""))
      .filter(!_.isEmpty)
      .flatMap(s => s.split("""\W+"""))
      .map(s => s.map(_.toLower))
    words.distinct(4).foreach(x => println(">>>" + x))
  }

  test("Case 4: Sort by will sort the information based on Ordering[T]") {
    val fileLocation = getClass.getResource("/rotj.txt").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 5)
    val words: RDD[String] = lines
      .flatMap(con => con.split("""\n"""))
      .filter(!_.isEmpty)
      .flatMap(s => s.split("""\W+"""))
      .map(s => s.map(_.toLower))

    words.distinct(4).sortBy(x => identity(x)).foreach(x => println(">>>" + x))
  }

  test("Case 5: Random RDD will split the RDDs by weight") {
    val fileLocation = getClass.getResource("/rotj.txt").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 5)
    val splitRDDs: Array[RDD[String]] = lines
      .flatMap(con => con.split("""\n"""))
      .filter(!_.isEmpty)
      .flatMap(s => s.split("""\W+"""))
      .map(s => s.map(_.toLower)).randomSplit(Array.apply(.5, .5))

    val array = splitRDDs.map {
      rdd => rdd.distinct(4).sortBy(x => identity(x))
    }
    val combined = array.reduce(_ ++ _)
    combined.foreach(x => println(">>>" + x))
  }

  test(
    """Case 6: Persist with a storage level, there are multiple storage levels, memory, or disk,
          and the replication factor of the storage. In this example the storage is both memory and disk
          and on at least 2 nodes""") {
    val fileLocation = getClass.getResource("/rotj.txt").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 5).setName("Lines to ROTJ")
    val words = lines
      .flatMap(_.split("""\n"""))
      .filter(!_.isEmpty)
      .flatMap(_.split("""\W+"""))
      .map(s => s.map(_.toLower)).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    println("Running initially")
    words.count()
    Thread.sleep(2000)
    println("Running again after 2 seconds")
    words.count()
    println("Going to remove all storage and block")
    words.unpersist(true)
    println("Running again after 2 seconds")
    Thread.sleep(2000)
    words.count()
    println("Done")
  }


  test(
    """Case 7: Caching will cache an RDD, Spark also supports pulling data sets
       into a cluster-wide in-memory cache. This is particularly helpful for hot datasets that
       are constantly queried that you can also query to the storage with getStorageLevel.
       It will not expire until Spark is out of memory, at which point it will
       remove RDDs from cache which are used least often. When you ask for something
       that has been uncached it will recalculate the pipeline and put it in cache again.""") {

    val fileLocation = getClass.getResource("/rotj.txt").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 5)
    val words = lines
      .flatMap(_.split("""\n"""))
      .filter(!_.isEmpty)
      .flatMap(_.split("""\W+"""))
      .map(s => s.map(_.toLower)).cache()
    words.count()
    words.getStorageLevel
    Thread.sleep(2000)
    println("Running again after 2 seconds")
    words.count()
    //Mark the RDD as non-persistent, and remove all blocks for it from memory and disk
    words.unpersist(true)
    println("Running again after 2 seconds")
    words.count()
  }

  test(
    """Case 8: Caching will cache an RDD, Spark also supports pulling data sets
       into a cluster-wide in-memory cache. cache is a synonym of persist or persist(MEMORY_ONLY)
       This is particularly helpful for hot datasets that
       It will not expire until Spark is out of memory, at which point it will
       remove RDDs from cache which are used least often. When you ask for something
       that has been uncached it will recalculate the pipeline and put it in cache again.""") {

    val fileLocation = getClass.getResource("/rotj.txt").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 5)
    val words = lines
      .flatMap(_.split("""\n"""))
      .filter(!_.isEmpty)
      .flatMap(_.split("""\W+"""))
      .map(s => s.map(_.toLower)).cache()
    words.count()
    words.getStorageLevel
    Thread.sleep(2000)
    println("Running again after 2 seconds")
    words.count()
    //Mark the RDD as non-persistent, and remove all blocks for it from memory and disk
    words.unpersist(true)
    println("Running again after 2 seconds")
    words.count
  }


  test("Case 9: reduce just operates like standard Scala by bringing all the content in by reduction") {
    val total = sparkContext.parallelize(1 to 5).reduce(_ * _)
    total should be(20)
  }

  test("Case 10: Convert from DataFrames or DataSet to RDD") {
    val dataSetLong = sparkSession.range(1, 100).map(x => x + 1)
    val dataFrames: DataFrame = dataSetLong.toDF("numbers")
    val rdd: RDD[Row] = dataFrames.rdd
    rdd.map(row => row.getInt(0)).foreach(x => println(x))
  }

  test("Case 10: Convert from DataFrames or DataSet to RDD") {
    val dataSetLong = sparkSession.range(1, 100).map(x => x + 1)
    val dataFrames: DataFrame = dataSetLong.toDF("numbers")
    val rdd: RDD[Row] = dataFrames.rdd
    rdd.map(row => row.getInt(0)).foreach(x => println(x))
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
