package com.xyzcorp

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.io.{BufferedSource, StdIn}
import scala.language.postfixOps
import scala.util.Random



object DataStreamSender extends App{

  implicit val system: ActorSystem = ActorSystem("MyActorSystem")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher


  //Be sure to match the port in Spark Streaming
  val socket = new Socket("127.0.0.1", 10150)
  println("Connecting")
  lazy val in = new BufferedSource(socket.getInputStream).getLines()
  val out = new PrintStream(socket.getOutputStream)
  out.println("Boom!")

  println("Message Sent")
//  val atomicInteger = new AtomicInteger(Random.nextInt(40))
//
//  Source.tick[Symbol](1 second, 1 millisecond, 'Tick).async.runForeach { x =>
//    val delta = -5 + Random.nextInt(10)
//    val change: Int = atomicInteger.intValue() + delta
//    if (change <= 0) {
//      atomicInteger.set(0)
//      out.println(0)
//      println("flushed" + 0)
//    } else {
//      atomicInteger.set(change)
//      out.println(change)
//      println("flushed" + change)
//    }
//    out.flush()
//  }

  out.close()
  socket.close()
  println("Done")
}
