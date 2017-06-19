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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 */

private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Double)
  extends Schedulable
  with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  var weight : Double = initWeight
  var minShare = initMinShare
  var runningTasks = 0
  var priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  var name = poolName
  var parent: Pool = null

  // add by cc
  var targetAlloc = 0.0
  var allResources = 32.0
  var alpha = 0.5
  var curve: scala.collection.mutable.Buffer[Double] = null
  var fairAlloc = 0.0;
  var minAlloc = 0.0;
  var nDemand = 0.0;
  var lSlope = 0.0;
  var rSlope = 0.0;
  var slopeArray: Array[Double] = null
  var lSlopeArray: Array[Double] = null
  var rSlopeArray: Array[Double] = null
  //
  def setCurve(curveString: String, clusterSize: String): Unit = {
//    logInfo("##### %d".format(curveString.length))
    val stringBuffer = curveString.split("-").toBuffer
//    logInfo("1##### %d".format(stringBuffer.length))
    parent.allResources = clusterSize.toDouble
    curve = stringBuffer.map{i => i.toDouble}
    nDemand = (curve.length-1) / weight;
    logInfo("##### poolName: " + poolName + "; curvelength: " + curve.length
      + "; weight: " + weight)
    //
    slopeArray = new Array[Double](curve.length);
    lSlopeArray = new Array[Double](curve.length);
    rSlopeArray = new Array[Double](curve.length);
//    logInfo("3##### %d".format(stringBuffer.length))
    //
    slopeArray(0) = 10.0;
//    logInfo("4##### %d".format(stringBuffer.length))
    slopeArray(curve.length-1) = 0;
    for (i <- 1 until curve.length - 1) {
      slopeArray(i) = curve(i +1) - curve(i -1);
    }
//    logInfo("2")
    //
    lSlopeArray(0) = 10.0;
    for (i <- 1 until curve.length) {
      lSlopeArray(i) = curve(i) - curve( i -1);
    }
    //
//    logInfo("3")
    rSlopeArray(curve.length-1) = 0;
    for (i <- 0 until curve.length-1) {
      rSlopeArray(i) = curve(i +1) - curve(i);
    }
  }
  //
  def init() {
    nDemand = (curve.length-1) / weight
    fairAlloc = 0.0
  }
  //
  def update_slope() {
    lSlope = lSlopeArray(targetAlloc.toInt);
    rSlope = rSlopeArray(targetAlloc.toInt);
  }
  //
  def getFairAlloc(): Unit = {
    val jobList_init = schedulableQueue.asScala.toBuffer[Schedulable].map{i => i.asInstanceOf[Pool]}
    var jobList = new ArrayBuffer[Pool]()
    val jobList_copy = new ArrayBuffer[Pool]()
    for (p <- jobList_init) {
      logInfo("##### haha the nDemand of job" + p.poolName + "-" + p.nDemand)
    }
    for ( i <- 0 until jobList_init.size) {
      if (jobList_init(i).poolName != "0"){
        jobList.append(jobList_init(i))
        jobList_copy.append(jobList_init(i))
      }
    }
    // need to check whether the adjustment of jobList could be reflected to Queue
    var totalResources = allResources
    var totalWeight = jobList.map{i => i.weight}.sum

    logInfo("##### the total slots: " + totalResources + "; totalweight: " + totalWeight)
    for (p <- jobList_copy) {
      logInfo("##### the nDemand of job" + p.poolName + "-" + p.nDemand)
    }

    jobList = jobList.sortBy(_.nDemand);
    for ( i <- 0 until jobList.length if totalResources > 0) {
      if ( i > 0 ) totalWeight -= jobList(i - 1).weight
      if (jobList(i).nDemand * totalWeight < totalResources) {
        totalResources -= jobList(i).nDemand * totalWeight;
        for (j <- i until jobList.length) {
          jobList(j).fairAlloc += jobList(i).nDemand * jobList(j).weight;
          jobList(j).nDemand -= jobList(i).nDemand;
        }
      } else {
        for (j <- i until jobList.length) {
          jobList(j).fairAlloc += totalResources/totalWeight * jobList(j).weight;
          jobList(j).nDemand -= totalResources/totalWeight;
        }
        totalResources = 0;
      }
    }
    for (p <- jobList_copy) {
      logInfo("##### the fair number of slot of job-" + p.poolName + "-" + p.fairAlloc)
    }
  }
  //
  def get_min_alloc(): Unit = {
    // this function can be called only after getFairAlloc
    var tmp_i = 1
    while (tmp_i <= fairAlloc.toInt && curve(fairAlloc.toInt - tmp_i) >=
      curve(fairAlloc.toInt) * alpha) {
      tmp_i += 1
    }
    minAlloc = fairAlloc - tmp_i + 1;
    // also initialize alloc
    targetAlloc = fairAlloc;
    logInfo("targetAlloc: " + targetAlloc + "; size of lSlopeArray: " + lSlopeArray.length)
    lSlope = lSlopeArray(targetAlloc.toInt);
    rSlope = rSlopeArray(targetAlloc.toInt);
  }
  //
  def getTargetShare(): Unit = {
    val jobList = new ArrayBuffer[Pool]()
    val jobList_copy = new ArrayBuffer[Pool]()
    val jobList_init = schedulableQueue.asScala.toBuffer[Schedulable].map{i => i.asInstanceOf[Pool]}
    for ( i <- 0 until jobList_init.size) {
      jobList_init(i).init()
      if (jobList_init(i).poolName != "0"){
        jobList.append(jobList_init(i))
        jobList_copy.append(jobList_init(i))
      }
    }
    getFairAlloc()
    if (jobList.size < 2) {
      this.targetAlloc = allResources
      return
    }
    for (p <- jobList) {
      p.get_min_alloc()
    }
    var setG = new ArrayBuffer[Pool]();
    var setH = new ArrayBuffer[Pool]();
    var setQ = new ArrayBuffer[Pool]();
    //
//    logInfo("##### hhh: %d".format(jobList.size))
    setG += jobList.minBy(_.lSlope);
    jobList -= jobList.minBy(_.lSlope);
    setH += jobList.maxBy(_.rSlope);
    jobList -= jobList.maxBy(_.rSlope);
    //
    var tmpGiverJob = setG.minBy(_.lSlope);
    var tmpGainJob = setH.maxBy(_.rSlope);
    var lMax = 0.0;
    var rMin = 0.0;
    var s = true;
    var stopCase_G = 0;
    var stopCase_H = 0;
    var shallDoAdjustment = false;
    //
    while (jobList.length != 0) {
      // terminate condition: jobList.length == 0 and no space for shifting
      // with one iteration, one giver job offers resources to one receiver job
      // all actions under a fixed jobList
      lMax = jobList.minBy(_.lSlope).lSlope; // set adjustment space
      rMin = jobList.maxBy(_.rSlope).rSlope;
      s = true;
      stopCase_G = 0;
      stopCase_H = 0;
      while (s) {
        // all actions under a fixed jobList, a fixed tmpGiver/GainJob.
        shallDoAdjustment = true;
        tmpGiverJob = setG.minBy(_.lSlope);
        if (tmpGiverJob.lSlope > lMax) {
          // upper bound reached
          s = false;
          stopCase_G = 1;
          shallDoAdjustment = false;
        }
        if (tmpGiverJob.targetAlloc == tmpGiverJob.minAlloc) {
          // if giverJob meets its fairness constrain
          setG -= tmpGiverJob;
          setQ += tmpGiverJob;
          shallDoAdjustment = false;
          if (setG.length == 0) {
            s = false;
            stopCase_G = 1;
          }
        }
        tmpGainJob = setH.maxBy(_.rSlope);
        if (tmpGainJob.rSlope < rMin) {
          // lower bound reached
          s = false;
          stopCase_H = 1;
          shallDoAdjustment = false;
        }
        // conduct adjustment
        if (shallDoAdjustment) {
          tmpGiverJob.targetAlloc -= 1;
          tmpGainJob.targetAlloc += 1;
          tmpGiverJob.update_slope();
          tmpGainJob.update_slope();
        }
      }
      // modify jobList according to the return value of while
      if (stopCase_G == 1) {
        // add new job to setG
        setG += jobList.minBy(_.lSlope);
        jobList -= jobList.minBy(_.lSlope);
      }
      if (stopCase_H == 1) {
        // add new job to setH
        // skip test whether jobList.length > 0
        setH += jobList.maxBy(_.rSlope);
        jobList -= jobList.maxBy(_.rSlope);
      }
    }
    while (s) {
      // terminate condition: no space for shifting
      // with one iteration, one giver job offers resources to one receiver job
      // all actions under a fixed jobList.
      // set adjustment space
      // when entering the loop, all the parameters must be usable
      tmpGiverJob = setG.minBy(_.lSlope);
      tmpGainJob = setH.maxBy(_.rSlope);
      shallDoAdjustment = true;
      logInfo("Enter iteration, tmpGiverJob: " + tmpGiverJob.poolName
        + " tmpGainJob: " + tmpGainJob.poolName)
      if (tmpGiverJob.lSlope >= tmpGainJob.rSlope) {
        logInfo("1")
        s = false;
        shallDoAdjustment = false;
      }
      if (tmpGiverJob.targetAlloc == tmpGiverJob.minAlloc) {
        logInfo("2")
        // if giverJob meets its fairness constrain
        // move tmpGiverJob to the finished (determined) set
        setG -= tmpGiverJob;
        setQ += tmpGiverJob;
        shallDoAdjustment = false;
        if (setG.length == 0) {
          logInfo("3")
          // no jobs can give out resources
          s = false;
        }
      }
      if (shallDoAdjustment) {
        logInfo("4")
        // conduct adjustment
        tmpGiverJob.targetAlloc -= 1;
        tmpGainJob.targetAlloc += 1;
        tmpGiverJob.update_slope();
        tmpGainJob.update_slope();
      }
    }
    for (p <- jobList_copy) {
      logInfo("##### the target number of slot of job-" + p.poolName + "-" + p.targetAlloc)
    }
  }

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case SchedulingMode.PAF =>
        new PAFSchedulingAlgorithm()
    }
  }

  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this

    // add by cc
    // calculate the target share when a new application is submitted
    if (schedulingMode == SchedulingMode.PAF) {
      getTargetShare()
    }
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)

    // add by cc
    // if current application finishes, then re_calculate the target share
    if (parent != null && parent.schedulingMode == SchedulingMode.PAF) {
      parent.removeSchedulable(this)
      parent.getTargetShare()
    }
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason) {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks()
    }
    shouldRevive
  }

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    if (schedulingMode == SchedulingMode.PAF) {
      for (taskSetManager <- sortedTaskSetQueue) {
        logInfo("##### ##### Print sortedResult in Queue: JobId-%d StageId-%d | priority-%d"
          .format(taskSetManager.jobId, taskSetManager.stageId,
            taskSetManager.priority))
      }
      logInfo("##### ##### End printing in PAF")
    }
    if (schedulingMode == SchedulingMode.FAIR) {
      for (taskSetManager <- sortedTaskSetQueue) {
        logInfo("##### ##### Print sortedResult in Queue: JobId-%d StageId-%d | priority-%d"
          .format(taskSetManager.jobId, taskSetManager.stageId,
            taskSetManager.priority))
      }
      logInfo("##### ##### End printing in FAIR")
    }
    sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
