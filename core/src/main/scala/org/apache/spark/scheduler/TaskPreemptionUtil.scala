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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging


object TaskPreemptionUtil extends Logging {

  def addPreemptedWeightageToExecID(execIdToPreempt: String): Unit = {
    executionIdVsPreemptWeightage.put(execIdToPreempt, execIdPreemptWeight.addAndGet(1))
  }

  val SPARK_EXECUTION_ID = "spark.execution.id"

  val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"

  val SPARK_PREEMPT_THRESHOLD = "spark.execution.preempt.threshold"

  // CoreUsage per execution Id
  private val executionIdVsCoreUsage = new mutable.HashMap[String, AtomicInteger]

  // taskId to executionID mapping
  private val taskIdVsExecutionID = new java.util.TreeMap[Long, String](Ordering[Long].reverse)

  private val executionIdVsWeightage = new mutable.HashMap[String, Long]

  private val executionIdVsPreemptWeightage = new mutable.HashMap[String, Long]

  // keeping track of the taskid s killled, inorder to prevent over killin of the same task
  private val killedTaskId = new mutable.HashSet[Long]

  // keeping track of the Preempted cores per execution Id
  private val executionIdVsPreemptedCores = new mutable.HashMap[String, AtomicInteger]

  val execIdWeight = new AtomicLong(0)

  val execIdPreemptWeight = new AtomicLong(0)

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
    logError("ExecutionUsageTracker: Record taskStart for" + taskId +
      " and execution id" + taskSetManager.sparkExecutionId.get)

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
      Option(props.get.getProperty(SPARK_EXECUTION_ID,
        props.get.getProperty(SPARK_SQL_EXECUTION_ID)))
    } else {
      None
    }
  }


  /*
  * Check if task Preemption can be done for the current ExecId
  * We check if min Core usage is defined - if yes, then we ll

  * * we ll check if the core usage for the execution id is less than min configured
   */
  private[scheduler] def canPreempt(execId: Option[String],
                                    taskSetManager: TaskSetManager): Boolean = synchronized {
    if (execId.isDefined && !executionIdVsCoreUsage.get(execId.get).isDefined
      && taskSetManager.preemptThreshold.isDefined
      && !taskSetManager.allPendingTasks.isEmpty) {
      logError(" CAn preempt is true ****** execId: " + execId)
      return true
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
    if (!executionIdVsCoreUsage.isEmpty) {
      executionIdVsCoreUsage.foreach(execId => {
        if (!execId.equals(executionId.get)) {
          logError("otherTaskToPreempt execId: " + execId)
          return true
        }
      })
    }
    false
  }


  def checkLocality(taskSetManager: TaskSetManager,
                    scheduler: TaskSchedulerImpl, taskId: Long): Boolean = {
    var canPreempt = false;
    if (!taskSetManager.pendingTasksForExecutor.isEmpty &&
      !taskSetManager.getPendingTasksForExecutor
      (scheduler.taskIdToExecutorId.get(taskId).get).isEmpty) {
      canPreempt = true;
    }

    if (!taskSetManager.pendingTasksForHost.isEmpty) {
      var host = scheduler.executorIdToHost(scheduler.taskIdToExecutorId.get(taskId).get)
      if (!taskSetManager.getPendingTasksForHost(host).isEmpty) {
        canPreempt |= true
      };
    }

    if (!taskSetManager.pendingTasksForRack.isEmpty) {
      var host = scheduler.executorIdToHost(scheduler.taskIdToExecutorId.get(taskId).get)
      if (scheduler.getRackForHost(host).isDefined
        && !taskSetManager.pendingTasksForRack(scheduler.getRackForHost(host).get).isEmpty) {
        canPreempt |= true
      };
    }
    if (!taskSetManager.pendingTasksWithNoPrefs.isEmpty) {
      canPreempt |= true
    }
    return canPreempt
  }

  /*
      * Fetch the TaskId to Preempt
       */
  private[scheduler] def getTaskIdToPreempt(taskSetManager: TaskSetManager, execId: String,
                                            scheduler: TaskSchedulerImpl): Long = {
    logError("****** getTaskIdToPreempt Logic Enter********* for ExecID " + execId)
      if (!execId.equals(taskSetManager.sparkExecutionId.get)) {
      import scala.collection.JavaConversions._
      for (entry <- taskIdVsExecutionID.entrySet) {
        logError("entry.getValue=" + entry.getValue)
        logError("!killedTaskId.contains(entry.getKey) " + !killedTaskId.contains(entry.getKey))
        logError("checkLocality(taskSetManager, scheduler, entry.getKey)=" +
          checkLocality(taskSetManager, scheduler, entry.getKey))
                if (entry.getValue.equals(execId) && !killedTaskId.contains(entry.getKey)
          && checkLocality(taskSetManager, scheduler, entry.getKey)) {
          return entry.getKey
        }
      }
    }
    -1
  }

  /*
  Using insertion Sort to Sort the taskSet based on weight
  as insertion sort works better than other sorts for smaller arrays.
  Sorting based in the weight assigned to the taskset, the weight is the mincore
  we sort in decending order of min core
   */
  def sortAccordingToWeight(sortedTaskSets: ArrayBuffer[TaskSetManager]):
  ArrayBuffer[TaskSetManager] = synchronized {
    if (executionIdVsWeightage.size > 0) {

      for (i <- 1 to sortedTaskSets.size - 1) {
        var max = sortedTaskSets(i)
        var j = i - 1
        while (j >= 0 && compareWeight(max.sparkExecutionId.get,
          sortedTaskSets(j).sparkExecutionId.get)) {
          sortedTaskSets(j + 1) = sortedTaskSets(j);
          j = j - 1;
        }
        sortedTaskSets(j + 1) = max;
      }
    }
    sortedTaskSets

  }

  // Gets the min no of tasks to preempt inorder to run the waiting task.
  private[scheduler] def getExecIDToPreempt
  (taskSetManager: TaskSetManager): String = synchronized {
    logError("****** getMincoreToPreempt Logic Enter*********")
    logError("Is threshold defined ===" + taskSetManager.preemptThreshold.isDefined)
    logError("Execution ID =" + taskSetManager.sparkExecutionId.get)
    logError("CoreUsage defined for thr execid " +
      executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).isDefined)
    val sortedExecutionUsage = (executionIdVsCoreUsage.toList.sortBy(_._2.get()).reverse).toIterator
    logError("Sorted CoreUsage " + executionIdVsCoreUsage.toList.sortBy(_._2.get()).reverse)
    while (sortedExecutionUsage.hasNext) {
      val execId = sortedExecutionUsage.next()
      if (!execId._1.equals(taskSetManager.sparkExecutionId.get)) {
        if (sortedExecutionUsage.hasNext) {
          val execIdNext = sortedExecutionUsage.next()
          if (execIdNext._2.get() == execId._2.get()) {
            if (executionIdVsPreemptWeightage.get(execId._1).isDefined) {
              if (executionIdVsPreemptWeightage.get(execIdNext._1).isDefined) {
                if (executionIdVsPreemptWeightage.get(execIdNext._1).get
                  > executionIdVsPreemptWeightage.get(execId._1).get) {
                  return execId._1
                }
              }
              return execIdNext._1
            }
          }
        }
        return execId._1
      }
    }
    null
  }


  // Gets the min no of tasks to preempt inorder to run the waiting task.
  private[scheduler] def getMincoreToPreempt(taskSetManager: TaskSetManager,
                                             execId: String): Int = synchronized {
    logError("****** getMincoreToPreempt Logic Enter*********")
    logError("Is threshold defined ===" + taskSetManager.preemptThreshold.isDefined)
    logError("To preempt Execution ID =" + execId)
    logError("CoreUsage defined for thr execid " +
      executionIdVsCoreUsage.get(execId).isDefined)
    if (taskSetManager.preemptThreshold.isDefined) {
      var cores = (executionIdVsCoreUsage(execId).get.toDouble
        * (taskSetManager.preemptThreshold.get.toDouble / 100))

      if (cores > 0) {
        return Math.floor(cores / taskSetManager.sched.CPUS_PER_TASK).toInt
      }
    }
    -1
  }

  private def getCoreUsageOnThreshold(taskSetManager: TaskSetManager, execId: String) = {
    (executionIdVsCoreUsage(execId).get.toDouble
      * (taskSetManager.preemptThreshold.get.toDouble / 100))
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

  /*
   private[scheduler] def canKillTaskId(taskSetManager: TaskSetManager): Boolean = {
     if (taskSetManager.sparkExecutionId.isDefined &&
       executionIdVsCoreUsage.get(taskSetManager.sparkExecutionId.get).isDefined) {
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
 */
  private[scheduler] def addPreemptWeight(taskSetManager: TaskSetManager): Unit = {

    executionIdVsWeightage.put(taskSetManager.sparkExecutionId.get, execIdWeight.getAndIncrement())
    logError("addPreemptWeight execId:" + taskSetManager.sparkExecutionId.get + " Weight =" +
      executionIdVsWeightage.get(taskSetManager.sparkExecutionId.get))
  }


}
