package com.xyzcorp

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkBasicRDDSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  private lazy val sparkConf = new SparkConf()
    .setAppName("spark_basic_rdd").setMaster("local[*]")
  private lazy val sparkSession = SparkSession.builder()
    .config(sparkConf).getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO")

  import sparkSession.implicits._ //required for conversions

  private lazy val logger = Logger.getLogger(this.getClass)


  def getAllWordsFromReturnOfTheJedi:RDD[String] = {
    val fileLocation = getClass.getResource("/rotj.txt").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 5)
    ???
  }

  test("""Case 1: Read from from a file and read the information
      |from the and count all the lengths. An RDD is read direct from a
      |sparkContext""") {

    val fileLocation = getClass.getResource("/goog.json").getPath
    val lines: RDD[String] = sparkContext.textFile(fileLocation, 3)
    val lineLengths: RDD[Int] = lines.map(s => s.length)
    val totalLength: Int = lineLengths.reduce((a, b) => a + b)
    totalLength should be(25560) //Total
  }

  test("Case 2: Parallelize will produce a stream of information across 4 " +
    "logical partitions")
  {
    val paralleled = sparkContext.parallelize(1 to 10, 4)
    val result = paralleled.map(x => x + 40).collect()
    logger.info("The result is %s".format(result))
    result should be(Array(41, 42, 43, 44, 45, 46, 47, 48, 49, 50))
  }

  test("Case 3: Distinct will retrieve all the content and show distinct items") {
    getAllWordsFromReturnOfTheJedi.distinct(4).foreach(println)
    pending
  }

  test("Case 4: Sort by will sort the information based on Ordering[T]") {
    getAllWordsFromReturnOfTheJedi.distinct(4)
       //.sortBy(???) //Sort by words
      .foreach(println)
    pending
  }

  test("""Case 5: Random RDD will split the RDDs by weight, see the results """ +
    "from this test") {
    val splitRDDs =  getAllWordsFromReturnOfTheJedi
      .randomSplit(Array.apply(.5, .5))

    splitRDDs.foreach(x => println(">>>" + x))
    pending
  }

  test(
    """Case 6: Persist with a storage level, there are multiple storage levels, memory, or disk,
          and the replication factor of the storage. In this example the storage is both memory and disk
          and on at least 2 nodes""") {
    val words = getAllWordsFromReturnOfTheJedi

    words.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
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
       that has been uncached it will recalculate the pipeline and put it in cache again.""")
  {

    val words = getAllWordsFromReturnOfTheJedi
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

  test("Case 10: Convert from DataFrames or DataSet to RDD") {
    val dataSetLong = sparkSession.range(1, 100).map(x => x + 1)
    val dataFrames: DataFrame = dataSetLong.toDF("numbers")
    val rdd: RDD[Row] = dataFrames.rdd
    rdd.map(row => row.getLong(0)).foreach(x => println(x))
  }

  test("Case 11: To DataFrame can take an RDD and convert to a DataFrame") {
    val rdd = sparkContext.parallelize(1 to 100)
    val dataFrame = rdd.toDF("amounts")
    val dataSet = dataFrame.map(row => row.getInt(0))
  }

  test("Case 12: Another example of converting to a DataFrame") {
    val afcNorth = Seq(("Bengals", "Cincinnati", "Paul Brown Stadium"),
      ("Steelers", "Pittsburgh", "Heinz Field"),
      ("Browns", "Cleveland", "FirstEnergy Field"),
      ("Ravens", "Baltimore", "M&T Bank Stadium"))
    val afcNorthDataFrame = afcNorth.toDF("NAME", "CITY", "STADIUM")
    afcNorthDataFrame.show
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
