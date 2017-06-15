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

package org.apache.spark.scheduler

import java.util.Properties

/**
 * A set of tasks submitted together to the low-level TaskScheduler, usually representing
 * missing partitions of a particular stage.
 */
private[spark] class TaskSet(
    val tasks: Array[Task[_]],
    val stageId: Int,
    val stageAttemptId: Int,
    val jobId: Int,
    val properties: Properties,
    val isFinal: Int = 1) {
  val id: String = stageId + "." + stageAttemptId
  val priority: Int = properties.getProperty("job.priority", "1000").toInt
  val isolationType: Int = properties.getProperty("job.isolationType", "2").toInt
  val threadId: Int = properties.getProperty("job.threadId", "0").toInt
  val estimatedRT: Int = properties.getProperty("job.estimatedRT", "50").toInt
  val alpha: Float = properties.getProperty("job.alpha", "1.6").toFloat
  val probability: Float = properties.getProperty("job.probability", "0.8").toFloat


  override def toString: String = "TaskSet " + id
}
