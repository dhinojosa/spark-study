package com.xyzcorp


import java.nio.file.Paths
import java.time._
import java.time.format.DateTimeFormatter

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class StandardPartitioner extends Partitioner {
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = {
    val s = key.asInstanceOf[String]
    if (s == "even") 0 else 1;
  }
}

class SparkAdvancedRDDSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private lazy val sparkConf = new SparkConf().setAppName("spark_basic_rdd").setMaster("local[*]")
  private lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  private lazy val sparkContext = sparkSession.sparkContext

  sparkContext.setLogLevel("INFO")

  test(
    """Case 1: Broadcast Variables allow the programmer to keep a read-only variable cached
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
    """Case 2: Accumulator Variables allow the programmer to keep a read-only variable cached
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
    """Case 3: Map Partitions. 	Similar to map, but runs separately on each
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
    """Case 4: Map Partition With Index. 	Similar to mapPartitions, but also
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
    """Case 5: Sample a fraction fraction of the data,
      with replacement, using a given random
      number generator seed. In this case we will be running with 10%""") {

    val data = sparkContext.parallelize(1 to 100)
                           .sample(withReplacement = true, fraction = 0.5)
    data.foreach(println)
  }


  test("""Case 6: Sample a fraction fraction of the data,
      without replacement, using a given random
      number generator seed. In this case we will be running with 10%""") {

    val data = sparkContext.parallelize(1 to 100)
      .sample(withReplacement = false, fraction = 0.5)
    data.foreach(println)
  }

  test(
    """Case 7: Union combines two RDDs of the same type, any identical elements will
         appear twice, use `distinct` to remove the data`""") {
     val data1 = sparkContext.parallelize(1 to 100)
     val data2 = sparkContext.parallelize(101 to 200)
     data1.union(data2).map(x => x * 20).foreach(println)
  }

  test("""Case 8: Intersection combines two RDDs and only contains the elements
      | shared by the same RDD""".stripMargin) {
    val data1 = sparkContext.parallelize(1 to 50)
    val data2 = sparkContext.parallelize(25 to 75)
    data1.intersection(data2).sortBy(identity).foreach(println)
  }

  test("""Case 9: Subtract""") {
    val data1 = sparkContext.parallelize(1 to 50)
    val data2 = sparkContext.parallelize(10 to 20)
    data1.subtract(data2).foreach(println)
  }

  test("""Case 10: A x B ={(a,b) | a ∈ A and b ∈ B}, but can cause memory pollution""") {
    val cartesian = sparkContext.parallelize(1 to 10)
                    .cartesian(sparkContext.parallelize('a' to 'z'))
    cartesian.foreach(println)
  }

  test(
    """Case 11: For each partition, will simply iterate over all the partitions of the data except the
      |function that we pass into foreachPartition is not expected
      |to have a return value.  This makes it great for doing something with each partition like writing it out to
      | a database. In fact, this is how many data source connectors are written""") {

    sparkContext.parallelize(1 to 1000).foreachPartition { iter =>
      import java.io._

      import scala.util.Random
      val randomFileName = new Random().nextInt()
      val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
      while (iter.hasNext) {
        pw.write(iter.next())
      }
      pw.close()
    }
  }


  test("""Case 12: Glom takes parallelized data and brings back data from each partition""") {
    sparkContext.parallelize(1 to 100, 5).map(x => x + 10).collect()
  }


  test("""Case 13: keyBy is a utility method that tuples rdd""") {
    val tuples = sparkContext.parallelize(1 to 100, 5).keyBy(i => i % 2 == 0).collect()
    println(tuples.toList)
  }

  test("""Case 14: partitionBy allows us to create our own Partitioner""") {
    val tuples = sparkContext.parallelize(1 to 100).keyBy(i => if (i % 2 == 0) "even" else "odd")
      .partitionBy(new StandardPartitioner).collect()
    println(tuples.toList)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    println("Press any enter to terminate")
    StdIn.readLine()
    sparkSession.stop()
    sparkContext.stop()
    super.afterAll()
  }

}
