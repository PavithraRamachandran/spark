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

package org.apache.spark.sql.execution

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExecutedCommandExec, ShowTablesCommand}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DateType, DecimalType, TimestampType, _}
import org.apache.spark.util.Utils

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) {

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }

  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = {
    if (sparkSession.sessionState.conf.getConf(SQLConf.USE_CONF_ON_RDD_OPERATION)) {
      new SQLExecutionRDD(executedPlan.execute(), sparkSession.sessionState.conf)
    } else {
      executedPlan.execute()
    }
  }

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf))



  protected def stringOrError[A](f: => A): String =
    try f.toString catch { case e: AnalysisException => e.toString }

  def simpleString: String = withRedaction {
    s"""== Physical Plan ==
       |${stringOrError(executedPlan.treeString(verbose = false))}
      """.stripMargin.trim
  }

  override def toString: String = withRedaction {
    def output = Utils.truncatedString(
      analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ")
    val analyzedPlan = Seq(
      stringOrError(output),
      stringOrError(analyzed.treeString(verbose = true))
    ).filter(_.nonEmpty).mkString("\n")

    s"""== Parsed Logical Plan ==
       |${stringOrError(logical.treeString(verbose = true))}
       |== Analyzed Logical Plan ==
       |$analyzedPlan
       |== Optimized Logical Plan ==
       |${stringOrError(optimizedPlan.treeString(verbose = true))}
       |== Physical Plan ==
       |${stringOrError(executedPlan.treeString(verbose = true))}
    """.stripMargin.trim
  }

  def stringWithStats: String = withRedaction {
    // trigger to compute stats for logical plans
    optimizedPlan.stats

    // only show optimized logical plan and physical plan
    s"""== Optimized Logical Plan ==
        |${stringOrError(optimizedPlan.treeString(verbose = true, addSuffix = true))}
        |== Physical Plan ==
        |${stringOrError(executedPlan.treeString(verbose = true))}
    """.stripMargin.trim
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }
  }
}
