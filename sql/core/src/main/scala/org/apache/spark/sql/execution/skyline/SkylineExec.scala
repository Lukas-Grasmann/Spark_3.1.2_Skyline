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

package org.apache.spark.sql.execution.skyline

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDistinct, SkylineItemOptions}
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, SparkPlan, UnaryExecNode}

case class SkylineExec(
  distinct: SkylineDistinct,
  skylineItemOptions: Seq[SkylineItemOptions],
  child: SparkPlan
) extends UnaryExecNode with CodegenSupport with PredicateHelper {

  override def output: Seq[Attribute] = child.output

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  /**
   * Returns all the RDDs of InternalRow which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  override def usedInputs: AttributeSet = { AttributeSet.empty }

  /**
   * Generate the Java source code to process, should be overridden by subclass to support codegen.
   *
   * doProduce() usually generate the framework, for example, aggregation could generate this:
   *
   * if (!initialized) {
   * # create a hash map, then build the aggregation hash map
   * # call child.produce()
   * initialized = true;
   * }
   * while (hashmap.hasNext()) {
   * row = hashmap.next();
   * # build the aggregation results
   * # create variables for results
   * # call consume(), which will call parent.doConsume()
   * if (shouldStop()) return;
   * }
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  /**
   * Generate the Java source code to process the rows from child SparkPlan. This should only be
   * called from `consume`.
   *
   * This should be override by subclass to support codegen.
   *
   * Note: The operator should not assume the existence of an outer processing loop,
   *       which it can jump from with "continue;"!
   *
   * For example, filter could generate this:
   *   # code to evaluate the predicate expression, result is isNull1 and value2
   *   if (!isNull1 && value2) {
   *     # call consume(), which will call parent.doConsume()
   *   }
   *
   * Note: A plan can either consume the rows as UnsafeRow (row), or a list of variables (input).
   *       When consuming as a listing of variables, the code to produce the input is already
   *       generated and `CodegenContext.currentVars` is already set. When consuming as UnsafeRow,
   *       implementations need to put `row.code` in the generated code and set
   *       `CodegenContext.INPUT_ROW` manually. Some plans may need more tweaks as they have
   *       different inputs(join build side, aggregate buffer, etc.), or other special cases.
   */
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    s"""
       | // consume start
       | ${consume(ctx, input)}
       | // consume end
     """.stripMargin

  /**
   * Currently ignores [[maxFields]] and falls back to [[toString]].
   *
   * @param maxFields Maximum number of fields that will be converted to strings.
   *                  Any elements beyond the limit will be dropped.
   * @return default string representation
   */
  override def simpleString(maxFields: Int): String = toString

  /**
   * Currently ignores [[maxFields]] and falls back to [[toString]].
   *
   * @param maxFields Maximum number of fields that will be converted to strings.
   *                  Any elements beyond the limit will be dropped.
   * @return default string representation
   */
  override def verboseString(maxFields: Int): String = toString

  override def toString: String = s"""
      | $formattedNodeName
      | ${ExplainUtils.generateFieldString("Input", child.output)}
      | ; Skyline dimensions: ${skylineItemOptions.mkString(", ")}
      | ; Distinct: ${distinct.distinct}
      |""".stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString
}
