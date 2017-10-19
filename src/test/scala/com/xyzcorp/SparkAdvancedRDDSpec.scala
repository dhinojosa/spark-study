package com.xyzcorp


import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time._
import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class SparkAdvancedRDDSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private lazy val sparkConf = new SparkConf().setAppName("spark_advanced_rdd").setMaster("local[*]")
  private lazy val sparkContext: SparkContext = new SparkContext(sparkConf)
  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._ //required for conversions

  test(
    """Case 1: Pipe method allows you to return an RDD created by piping elements
        to a forked external process. The resulting RDD is computed by executing the
        given process once per partition. All elements of each input partition are
        written to a process’s stdin as lines of input separated by a newline.
        The resulting partition consists of the process’s stdout output, with each line
        of stdout resulting in one element of the output partition. A process is invoked
        even for empty partitions.""") {

    val userHome = System.getProperty("user.home")

    //val value1: RDD[String] = sparkContext.parallelize(1 to 10).pipe(seq)
  }

  test(
    """Case 2: Broadcast Variables allow the programmer to keep a read-only variable cached
        on each machine rather than shipping a copy of it with tasks, distributed in an
        efficient manner. Distribution is done efficiently to reduce overhead""") {
    val broadcast: Broadcast[Seq[ZoneId]] =
      sparkContext.broadcast(Seq(ZoneId.of("America/New_York"),
        ZoneId.of("America/Los_Angeles")))

    val movies = sparkContext.parallelize(Seq("1PM", "2PM", "3PM"))
      .map(s => LocalDateTime.of(LocalDate.now(),
        LocalTime.parse(s, DateTimeFormatter.ofPattern("ha"))))
      .flatMap(ldt => broadcast.value.map(zid => ZonedDateTime.of(ldt, zid)))
    movies.collect().foreach(println)
  }


  test(
    """Case 3: Accumulator Variables allow the programmer to keep a read-only variable cached
        on each machine rather than shipping a copy of it with tasks, distributed in an
        efficient manner. Distribution is done efficiently to reduce overhead""") {

    println(sparkContext.uiWebUrl)
    val accumulator = sparkContext.collectionAccumulator[Long]("worker-disk-drive-space")
    accumulator.reset()

    val collection = sparkContext.parallelize(1 to 10000).map(x => {
      accumulator.add(Paths.get(System.getProperty("user.home")).toFile.getTotalSpace)
      x + 1
    }).collect() //We need to terminate with an action

    import scala.collection.JavaConverters._
    val list = accumulator.value.asScala.toList
    println(list.filter(_ < 10000))
  }

  test(
    """Case 4: Map Partitions. 	Similar to map, but runs separately on each
       partition (block) of the RDD, so func must be of type Iterator<T> => Iterator<U>
       when running on an RDD of type T. preservesPartitioning`, the second parameter,
       should be `false` unless this is a pair RDD and the input function
       doesn't modify the keys indicates whether the input function preserves
       the partitioner, which.""") {

    val data: Array[Int] = sparkContext.parallelize(1 to 100)
      .mapPartitions(it => it.map(_ + 10)).collect
    println(data.toList)
  }

  test(
    """Case 5: Map Partition With Index. 	Similar to mapPartitions, but also
      provides func with an integer value representing the index of the partition,
      so func must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD
      of type T.""") {

    val data = sparkContext.parallelize(1 to 100)
      .mapPartitionsWithIndex(
        (idx, it) => it.map(n => s"Received $n on partition index: $idx"))
      .collect()
    data.foreach(println)
  }

  test(
    """Case 6: Sample a fraction fraction of the data,
      with replacement, using a given random
      number generator seed. In this case we will be running with 10%""") {

    val data = sparkContext.parallelize(1 to 100)
                           .sample(withReplacement = true, fraction = 0.5)
    data.foreach(println)
  }


  test("""Case 7: Sample a fraction fraction of the data,
      without replacement, using a given random
      number generator seed. In this case we will be running with 10%""") {

    val data = sparkContext.parallelize(1 to 100)
      .sample(withReplacement = false, fraction = 0.5)
    data.foreach(println)
  }

  test(
    """Case 8: Union combines two RDDs of the same type, any identical elements will
         appear twice, use `distinct` to remove the data`""") {
     val data1 = sparkContext.parallelize(1 to 100)
     val data2 = sparkContext.parallelize(101 to 200)
     data1.union(data2).map(x => x * 20).foreach(println)
  }

  test("""Case 9: Intersection combines two RDDs and only contains the elements
      | shared by the same RDD""".stripMargin) {
    val data1 = sparkContext.parallelize(1 to 50)
    val data2 = sparkContext.parallelize(25 to 75)
    data1.intersection(data2).sortBy(identity).foreach(println)
  }





  test("Case X: Using Kyro for Serialization") {

  }


  test("Case Y: Using Avro for Serialization") {

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
