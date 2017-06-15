/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark._

import scala.collection.mutable.HashMap

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.concurrent.future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

/** Computes an approximation to pi */
object LocalPi {
  def waiting(time: Int) : Unit = {
    // time in microsecond
    val start = System.nanoTime()
    while (System.nanoTime() < start + time * 10000000L){}
  }

  def waitMap(time: Int, arg: (Int, Int)): (Int, Int) = {
    waiting(time)
    arg
  }

  def job0(spark: SparkContext): Unit = {
    spark.setLocalProperty("job.priority", "0")
    spark.setLocalProperty("job.threadId", "0")
    spark.setLocalProperty("job.isolationType", "0")
    val value0 = spark.parallelize(0 until 16, 2).map(i => (i, i)).map(i => waitMap(40, i))
    val value = value0.reduceByKey( (x, y) => x + y, 2).map(i => waitMap(40, i))
    value.collect()
    val value3 = spark.parallelize(0 until 16, 2).map(i => (i, i)).map(i => waitMap(40, i))
    val value4 = value3.reduceByKey( (x, y) => x + y, 2).map(i => waitMap(40, i))
    value4.collect()
//    spark.stopReserve(0)
    spark.cancelJobGroup("0")
  }

  def job1(spark: SparkContext): Unit = {
    spark.setLocalProperty("job.priority", "1")
    spark.setLocalProperty("job.threadId", "202")
    spark.setLocalProperty("job.isolationType", "2")
    val value01 = spark.parallelize(0 until 16, 2).map(i => (i % 4, i)).map(i => waitMap(40, i))
    val value = value01.reduceByKey( (x, y) => x + y, 2).map(i => waitMap(40, i))
    value.collect()

//    val value02 = value01.reduceByKey((x, y) => x + y, 2)
//    val value03 = value02.map( i => (i._1 % 2, i._2)).map(i => waitMap(20, i))
//    val value04 = value03.reduceByKey((x, y) => x + y, 2)
//
//    val value11 = spark.parallelize(0 until 4, 2).map(i => (i % 2, i)).map(i => waitMap(20, i))
//    val value12 = value11.reduceByKey((x, y) => x + y, 2)
//    val value = value12.union(value04).map(i => waitMap(10, i))
  }

  def job2(spark: SparkContext): rdd.RDD[(Int, Int)] = {
    spark.setLocalProperty("spark.scheduler.pool", "2")
    val value = spark.parallelize(0 until 4, 4).map(i => (i, i)).map(i => waitMap(50, i))
    value
  }

  def submit(sc: SparkContext, submittingTime: Int,
             i: Int): Unit = {
    waiting(submittingTime)
    sc.setLocalProperty("spark.phaseInterval", "50")
    if (i == 0){
      job0(sc)
    }
    if (i == 1){
      job1(sc)
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").set("spark.scheduler.mode", "FIFO")
      .set("spark.eventLog.enabled", "true").set("spark.executor.cores", "1")
    val spark = new SparkContext(conf)


    Array((0, 0), (50, 1)).par.foreach(i =>
      submit(spark, i._1, i._2))

    spark.stop()
  }
}

