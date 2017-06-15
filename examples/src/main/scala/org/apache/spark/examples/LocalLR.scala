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

import java.util.Random

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
 * Logistic regression based classification.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
 * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
 */
object LocalLR {
  def waiting(time: Int) : Unit = {
    // time in microsecond
    val start = System.nanoTime()
    while (System.nanoTime() < start + time * 1000000L) {}
  }

  def waitMap(time: Int, arg: (Int, Int)): (Int, Int) = {
    waiting(time)
    arg
  }

  def job0(spark: SparkContext): rdd.RDD[(Int, Int)] = {
    spark.setLocalProperty("application.ID", "1")
    spark.setLocalProperty("application.Weight", "1")
    spark.setLocalProperty("application.CurveString", "0-0.2-0.5-0.8-1")
    spark.setLocalProperty("spark.scheduler.pool", "0")
    waiting(1500)
    val value = spark.parallelize(0 until 4, 4).map(i => (i, i)).map(i => waitMap(2000, i))
    value
  }

  def job1(spark: SparkContext): rdd.RDD[(Int, Int)] = {
    spark.setLocalProperty("application.ID", "2")
    spark.setLocalProperty("application.Weight", "2")
    spark.setLocalProperty("application.CurveString", "0-0.1-0.2-0.3-1")
    spark.setLocalProperty("spark.scheduler.pool", "1")
    waiting(1500)
    val value = spark.parallelize(0 until 4, 4).map(i => (i, i)).map(i => waitMap(2000, i))
    value
  }

  def job2(spark: SparkContext): rdd.RDD[(Int, Int)] = {
    val value = spark.parallelize(0 until 2, 2).map(i => (i, i))
      .map(i => waitMap(500 + 500*i._1, i))
    value
  }

  def submit(sc: SparkContext, submittingTime: Int,
             i: Int): Unit = {
    waiting(submittingTime)
    sc.setLocalProperty("spark.phaseInterval", "50")
    if (i == 0) {
      job0(sc).collect()
    }
    if (i == 1) {
      job1(sc).collect()
    }
    if (i == 2) {
      job2(sc).collect()
    }
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").set("spark.scheduler.mode", "FAIR")
      .set("spark.executor.cores", "1")
    val spark = new SparkContext(conf)
    // in real test, these properties in each thread shall be set in priori
    spark.setLocalProperty("cluster.size", "4")
    // It remains a problem how to set the cluster size. Currently it's still 32.


    Array((0, 0), (5, 1), (2, 2)).par.foreach(i =>
      submit(spark, i._1, i._2))

    spark.stop()
  }
}
// scalastyle:on println
