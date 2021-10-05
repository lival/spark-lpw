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

import java.io._
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
  */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[U](stageId, stageAttemptId, partition.index, localProperties, serializedTaskMetrics,
    jobId, appId, appAttemptId)
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  // instrument code
  def calculateTrueTime(finalRdd: RDD[_]): mutable.HashMap[Int, Long] = {
    val result = new mutable.HashMap[Int, Long]()
    val toVisit = new mutable.Queue[RDD[_]]()
    val dependenciesToCalculate = new mutable.Stack[RDD[_]]()
    var rdd = finalRdd
    toVisit.enqueue(rdd)
    while (toVisit.nonEmpty) {
      rdd = toVisit.dequeue()
      dependenciesToCalculate.push(rdd)
      for (depRdd <- rdd.dependencies) {
        if (depRdd.rdd != null) {
          if (!dependenciesToCalculate.contains(depRdd.rdd)) {
            dependenciesToCalculate.push(depRdd.rdd)
            toVisit.enqueue(depRdd.rdd)
          }
        }
      }
    }
    while (dependenciesToCalculate.nonEmpty) {
      rdd = dependenciesToCalculate.pop()
      if (rdd.partitionCost != -1) {
      }
      else if (rdd.dependencies.isEmpty) {
        rdd.partitionCost = rdd.timestampEnd - rdd.timestampStart
        result.put(rdd.id, rdd.partitionCost)
      }
      // val startTimeSet: mutable.Set[Long] = mutable.Set[Long](rdd.timestampStart)
      var startTime: Long = rdd.timestampStart
      for (depRdd <- rdd.dependencies) {
        if (depRdd.rdd != null) {
          // startTimeSet.add(depRdd.rdd.timestampEnd)
          if (depRdd.rdd.timestampEnd > startTime) {
            startTime = depRdd.rdd.timestampEnd
          }
        }
      }
      // val startTime = startTimeSet.max
      if (startTime > rdd.timestampEnd) {
        rdd.partitionCost = 0
        result.put(rdd.id, rdd.partitionCost)
      }
      else {
        rdd.partitionCost = rdd.timestampEnd - startTime
        result.put(rdd.id, rdd.partitionCost)
      }
    }
    /* while (toVisit.nonEmpty) {
      rdd = toVisit.dequeue()
      dependenciesToCalculate.push(rdd)
      for (depRdd <- rdd.dependencies) {
        if (depRdd.rdd != null) {
          toVisit.enqueue(depRdd.rdd)
          if (!dependenciesToCalculate.contains(depRdd.rdd)) {
            dependenciesToCalculate.push(depRdd.rdd)
          }
        }
      }
    }
    while (dependenciesToCalculate.nonEmpty) {
      rdd = dependenciesToCalculate.pop()
      if (rdd.partitionCost != -1) {
      }
      else if (rdd.dependencies.isEmpty) {
        rdd.partitionCost = rdd.partitionIdtoCostWithDep
        result.put(rdd.id, rdd.partitionIdtoCostWithDep)
      }
      else {
        var tempPast = new mutable.HashMap[Int, Long]()
        for (past <- rdd.dependencies) {
          if (past.rdd != null) {
            tempPast.put(past.rdd.id, past.rdd.partitionCost)
            tempPast = tempPast ++ past.rdd.pastPartitionCost
          }
        }
        rdd.pastPartitionCost = tempPast
        val pastcost = tempPast.values.toArray.sum
        val costWithDep = rdd.partitionIdtoCostWithDep
        if ((costWithDep - pastcost) >= 0) {
          rdd.partitionCost = (costWithDep - pastcost)
          result.put(rdd.id, costWithDep - pastcost)
        }
        else {
          rdd.partitionCost = 0
          result.put(rdd.id, 0)
        }
      }
    } */
    result
  }
  // instrument code end

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val result = func(context, rdd.iterator(partition, context))
   // val timestampStart = System.currentTimeMillis()
    val map = calculateTrueTime(rdd)
  //  val costTime = System.currentTimeMillis() - timestampStart
   // println("result task cost calculation time is " + costTime.toString + "ms")
   // println(map.toString())
    SparkEnv.get.blockManager.memoryStore.updateCost(partition.index, map)
    result
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}
