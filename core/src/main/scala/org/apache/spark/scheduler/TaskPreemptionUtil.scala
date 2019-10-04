package org.apache.spark.scheduler

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.TaskPreemptionUtil.executionIdVsCoreUsage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TaskPreemptionUtil extends Logging {


  val SPARK_EXECUTION_ID = "spark.execution.id"
  val SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id"
  val SPARK_EXECUTION_MIN_CORE = "spark.execution.min.core"

  private val executionIdVsCoreUsage = new mutable.HashMap[String, AtomicInteger]

  private val executionIdVsWeightage = new mutable.HashMap[String, Int]

  private val executionIdVsMinCoreUsage = new mutable.TreeSet[String]

  private val taskIdVsExecutionID = new mutable.HashMap[Long, String]

  private val killedTaskId = new mutable.HashSet[Long]

  private val executionIdVsPreemtTime = new mutable.HashMap[String, Long]


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

  // adding to executionIdVsMinCoreUsage those execid whose core usage is > than its min core configured
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
        taskIdVsExecutionID.remove(taskId).isDefined &&
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
        executionIdVsWeightage.remove(taskSetManager.sparkExecutionId.get)
      }
    }
    killedTaskId.remove(taskId)
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
                                    taskSetManager: TaskSetManager): Boolean = {
    if (execId.isDefined &&
      (!executionIdVsPreemtTime.get(execId.get).isDefined || isPreemptTimeExceedLimit(execId.get))) {
      if (taskSetManager.mincoreUsageCap.isDefined) {
        if (executionIdVsCoreUsage.get(execId.get).isDefined) {
          return executionIdVsCoreUsage.get(execId.get).get.get < taskSetManager.mincoreUsageCap.get
        }
        return true
      }
    }
    false
  }

  private[scheduler] def compareWeight(execId1: String,
                                       execId2: String): Boolean = {
    if (executionIdVsWeightage.get(execId1).isDefined) {
      if (executionIdVsWeightage.get(execId2).isDefined) {
        return executionIdVsWeightage.get(execId1).get < executionIdVsWeightage.get(execId2).get
      }
      return false
    }
    true
  }

  /*
      Checking if any other execution Id is other than the current one is present with
       core usage above its assigned min is available
 */
  private[scheduler] def otherTaskToPreempt(executionId: Option[String], taskSetManager: TaskSetManager): Boolean = {
    if (!executionIdVsMinCoreUsage.isEmpty) {
      executionIdVsMinCoreUsage.foreach(execId => {
        if (!execId.equals(executionId.get)) {
          if (taskSetManager.mincoreUsageCap.isDefined && !executionIdVsWeightage.get(executionId.get).isDefined) {
            executionIdVsWeightage.put(executionId.get, taskSetManager.mincoreUsageCap.get)
          }
          return true
        }
      })
    }
    false
  }

  /*
  * Fetch the TaskId to Preempt
   */
  private[scheduler] def getTaskIdToPreempt(executionId: Option[String]): Long = {
    executionIdVsMinCoreUsage.foreach(execId => {
      if (!execId.equals(executionId)) {
        taskIdVsExecutionID.foreach(taskId => {
          if (taskId._2.equals(execId) && !killedTaskId.contains(taskId._1)) {
            return taskId._1
          }
        })
      }
    }
    )
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
      for (i <- 1 to sortedTaskSets.size) {
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
  private[scheduler] def addKilledTaskId(taskId: Long)
  : Unit = synchronized {
    killedTaskId.add(taskId)
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

    (System.currentTimeMillis() - executionIdVsPreemtTime.get(execId).get) > (120 * 1000)

  }
}
