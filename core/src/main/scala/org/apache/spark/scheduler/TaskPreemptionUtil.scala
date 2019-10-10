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

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskPreemptionUtil extends Logging {


  val SPARK_EXECUTION_ID = "spark.execution.id"
  val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"
  val SPARK_EXECUTION_MIN_CORE = "spark.execution.min.core"

  //CoreUsage per execution Id
  private val executionIdVsCoreUsage = new mutable.HashMap[String, AtomicInteger]

  //Storing the execution Id whose coreusage is more than the min Configured
  private val executionIdVsMinCoreUsage = new mutable.TreeSet[String]

  //taskId to executionID mapping
  private val taskIdVsExecutionID = new java.util.TreeMap[Long, String](Ordering[Long].reverse)

  private val executionIdVsWeightage = new mutable.HashMap[String, Int]

  //keeping track of the taskid s killled, inorder to prevent over killin of the same task
  private val killedTaskId = new mutable.HashSet[Long]

  private val executionIdVsPreemtTime = new mutable.HashMap[String, Long]

  //keeping track of the Preempted cores per execution Id
  private val executionIdVsPreemptedCores = new mutable.HashMap[String, AtomicInteger]

  private[scheduler] def onTaskStart(taskId: Long,
                                     taskSetManager: TaskSetManager): Unit = synchronized {
    if (taskSetManager.sparkExecutionId.isDefined) {
      taskIdVsExecutionID.put(taskId, taskSetManager.sparkExecutionId.get)
      if (executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).isEmpty) {
        executionIdVsCoreUsage.put(taskSetManager.sparkExecutionId.get
          , new AtomicInteger(taskSetManager.sched.CPUS_PER_TASK))
      }
      else {
        executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).get.
          addAndGet((+taskSetManager.sched.CPUS_PER_TASK))
      }
    }
    logDebug("ExecutionUsageTracker: Record taskStart for" + taskId +
      " and execution id" + taskSetManager.sparkExecutionId.get)
  }

  // adding to executionIdVsMinCoreUsage those execid whose core usage is > than its min core configured S
  private[scheduler] def addToMinCoreUsageSet(taskSetManager: TaskSetManager) = synchronized {
    if (taskSetManager.mincoreUsageCap.isDefined &&
      executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).get.get
        > taskSetManager.mincoreUsageCap.get) {
      executionIdVsMinCoreUsage.add(taskSetManager.sparkExecutionId.get)
      executionIdVsPreemtTime.remove(taskSetManager.sparkExecutionId.get)
      logDebug("TaskPreemptionUtil: Record execution id:" + taskSetManager.sparkExecutionId.get
        + " reached Min Core Usage of " + taskSetManager.mincoreUsageCap.get)
    }
  }

  private[scheduler] def onTaskEnd(taskId: Long,
                                   taskSetManager: TaskSetManager): Unit = synchronized {

    taskSetManager.sparkExecutionId.foreach(execId => {
      if (executionIdVsCoreUsage.get(execId).isDefined &&
        null != taskIdVsExecutionID.remove(taskId) &&
        executionIdVsCoreUsage.get(execId).get.
          addAndGet(-taskSetManager.sched.CPUS_PER_TASK) == 0) {
        executionIdVsCoreUsage.remove(execId)
      }
      logDebug("ExecutionUsageTracker: Record end Task for" + taskId)
    })
  }

  private[scheduler] def removeFromMinCoreUsageSet(taskId: Long, taskSetManager: TaskSetManager) = synchronized {
    if (taskSetManager.mincoreUsageCap.isDefined &&
      executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).isDefined) {
      if (executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).get.get
        < taskSetManager.mincoreUsageCap.get) {
        executionIdVsMinCoreUsage.remove(taskSetManager.sparkExecutionId.get)
      }

      if (killedTaskId.contains(taskId)) {
        killedTaskId.remove(taskId)
        if (executionIdVsPreemptedCores.get(taskSetManager.sparkExecutionId.get).
          get.addAndGet(-taskSetManager.sched.CPUS_PER_TASK) == 0) {
          executionIdVsPreemptedCores.remove(taskSetManager.sparkExecutionId.get)
        }
      }

      if (!executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).isDefined) {
        executionIdVsPreemtTime.remove(taskSetManager.sparkExecutionId.get)
        //executionIdVsWeightage.remove(taskSetManager.sparkExecutionId.get)
      }
    }
  }

  def getInt(props: Option[Properties], key: String): Option[Int] = {
    if (props.isDefined) {
      val value = props.get.getProperty(key)
      if (value != null) {
        try {
          return Option(value.toInt)
        }
        catch {
          case e: NumberFormatException =>
            logDebug("property resulted " + key)
        }
      }
    }
    None
  }

  def getExecutionId(props: Option[Properties]): Option[String] = {
    if (props.isDefined) {
      return Option(props.get.getProperty(SPARK_EXECUTION_ID, props.get.getProperty(SPARK_SQL_EXECUTION_ID)))
    } else {
      None
    }
  }

  private[scheduler] def getMinCores(props: Option[Properties]): Option[Int] = {
    getInt(props, SPARK_EXECUTION_MIN_CORE).filter(_ > 0)
  }

  /*
  * Check if task Preemption can be done for the current ExecId
  * We check if min Core usage is defined - if yes, then we ll
  * we ll check if the core usage for the execution id is less than min configured
   */
  private[scheduler] def canPreempt(execId: Option[String],
                                    taskSetManager: TaskSetManager): Boolean = synchronized {
    if (execId.isDefined &&
      (!executionIdVsPreemtTime.get(execId.get).isDefined || isPreemptTimeExceedLimit(execId.get))) {
      if (taskSetManager.mincoreUsageCap.isDefined) {
        if (executionIdVsCoreUsage.get(execId.get).isDefined) {
          logError(" canPreempt for execId = " + execId + "Core Usage = " + executionIdVsCoreUsage.get(execId.get).get)
          return executionIdVsCoreUsage.get(execId.get).get.get < taskSetManager.mincoreUsageCap.get
        }
        return true
      }
    }
    false
  }

  private[scheduler] def compareWeight(execId1: String,
                                       execId2: String): Boolean = synchronized {
    if (executionIdVsWeightage.get(execId1).isDefined) {
      if (executionIdVsWeightage.get(execId2).isDefined) {
        return executionIdVsWeightage.get(execId1).get > executionIdVsWeightage.get(execId2).get
      }
      return true
    }
    false
  }

  /*
      Checking if any other execution Id is other than the current one is present with
       core usage above its assigned min is available
 */
  private[scheduler] def otherTaskToPreempt(executionId: Option[String]): Boolean = {
    if (!executionIdVsMinCoreUsage.isEmpty) {
      executionIdVsMinCoreUsage.foreach(execId => {
        if (!execId.equals(executionId.get)) {
          return true
        }
      })
    }
    false
  }

  /*
  * Fetch the TaskId to Preempt
   */
  private[scheduler] def getTaskIdToPreempt(taskSetManager: TaskSetManager, scheduler: TaskSchedulerImpl): Long = {
    executionIdVsMinCoreUsage.foreach(execId => {
      if (!execId.equals(taskSetManager.sparkExecutionId.get)) {
        import scala.collection.JavaConversions._
        for (entry <- taskIdVsExecutionID.entrySet) {
          if (entry.getValue.equals(execId) && !killedTaskId.contains(entry.getKey) &&
            canKillTaskId(scheduler.taskIdToTaskSetManager.get(entry.getKey))) {
            if (taskSetManager.mincoreUsageCap.isDefined &&
              !executionIdVsWeightage.get(taskSetManager.sparkExecutionId.get).isDefined) {
              executionIdVsWeightage.put(taskSetManager.sparkExecutionId.get, taskSetManager.mincoreUsageCap.get)
            }
            return entry.getKey
          }
        }
      }
    })
    -1
  }

  /*
  Using insertion Sort to Sort the taskSet based on weight
  as insertion sort works better than other sorts for smaller arrays.
  Sorting based in the weight assigned to the taskset, the weight is the mincore
  we sort in decending order of min core
   */
  def sortAccordingToWeight(sortedTaskSets: ArrayBuffer[TaskSetManager]): ArrayBuffer[TaskSetManager] = synchronized {
    if (executionIdVsWeightage.size > 0) {

      for (i <- 1 to sortedTaskSets.size - 1) {
        var max = sortedTaskSets(i)
        var j = i - 1
        while (j >= 0 && compareWeight(max.sparkExecutionId.get, sortedTaskSets(j).sparkExecutionId.get)) {
          sortedTaskSets(j + 1) = sortedTaskSets(j);
          j = j - 1;
        }
        sortedTaskSets(j + 1) = max;
      }
    }
    sortedTaskSets

  }

  //Gets the min no of tasks to preempt inorder to run the waiting task.
  private[scheduler] def getMincoreToPreempt(taskSetManager: TaskSetManager): Int = {
    if (taskSetManager.mincoreUsageCap.isDefined) {
      var cores = taskSetManager.mincoreUsageCap.get.toInt
      if (executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).isDefined) {
        cores -= executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).get.get
      }
      if (cores > 0) {
        return cores / taskSetManager.sched.CPUS_PER_TASK
      }
    }
    -1
  }

  // addin taskid to the set inorder to avaoid killing the same task again and again.
  private[scheduler] def addKilledTaskId(taskId: Long, taskSetManager: TaskSetManager)
  : Unit = synchronized {
    killedTaskId.add(taskId)
    if (taskSetManager.sparkExecutionId.isDefined) {
      if (executionIdVsPreemptedCores.get(taskSetManager.sparkExecutionId.get).isDefined) {
        executionIdVsPreemptedCores.get(taskSetManager.sparkExecutionId.get).get.
          addAndGet(taskSetManager.sched.CPUS_PER_TASK)
      }
      else {
        executionIdVsPreemptedCores.put(taskSetManager.sparkExecutionId.get,
          new AtomicInteger(taskSetManager.sched.CPUS_PER_TASK))
      }
    }
  }

  //maintaing a map of execution id vs time at which preemption was done. In order to avoid
  //over killing of tasks for the same execution id.
  private[scheduler] def addPreempTime(execId: String)
  : Unit = synchronized {
    if (executionIdVsPreemtTime.get(execId).isDefined) {
      if (isPreemptTimeExceedLimit(execId)) {
        executionIdVsPreemtTime.put(execId, System.currentTimeMillis())
      }
    }
    else {
      executionIdVsPreemtTime.put(execId, System.currentTimeMillis())
    }
  }

  //checking if the
  private[scheduler] def isPreemptTimeExceedLimit(execId: String)
  : Boolean = synchronized {

    (System.currentTimeMillis() - executionIdVsPreemtTime.get(execId).get) > (1 * 1000)

  }

  private[scheduler] def canKillTaskId(taskSetManager: TaskSetManager): Boolean = {
    if (taskSetManager.sparkExecutionId.isDefined &&
      executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).isDefined
      && taskSetManager.mincoreUsageCap.isDefined) {
      if (executionIdVsPreemptedCores.get(taskSetManager.sparkExecutionId.get).isDefined) {
        return ((executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).get.get
          - taskSetManager.sched.CPUS_PER_TASK
          - executionIdVsPreemptedCores.get(taskSetManager.sparkExecutionId.get).get.get)
          >= taskSetManager.mincoreUsageCap.get)
      }
      else {
        return (executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).get.get
          - taskSetManager.sched.CPUS_PER_TASK >= taskSetManager.mincoreUsageCap.get)
      }

    }

    false
  }

}
