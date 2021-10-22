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

import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDimension, SkylineDistinct, SkylineIsDistinct}
import org.apache.spark.sql.execution.{AliasAwareOutputPartitioning, ExplainUtils, UnaryExecNode}

/**
 * Base trait common to all skyline physical plan (execution) nodes.
 *
 * Note that multiple skyline physical plan nodes may be used to express a single skyline operator.
 * This is, for example, the case when using a local skyline followed by a global skyline where
 * both local and global skyline are instances of a descendant of [[BaseSkylineExec]].
 */
trait BaseSkylineExec extends UnaryExecNode with AliasAwareOutputPartitioning {
  // Whether the skyline is distinct as [[SkylineDistinct]]
  def skylineDistinct: SkylineDistinct
  // Accessor as to whether the skyline (true) or not (false) as Boolean
  def skylineDistinctBoolean: Boolean =
    skylineDistinct == SkylineIsDistinct

  // Sequence of [[SkylineDimension]] that hold the dimensions
  def skylineDimensions: Seq[SkylineDimension]
  // Get the named expressions of the skyline dimensions
  def skylineDimensionExpressions: Seq[NamedExpression] =
    skylineDimensions.map(_.child.asInstanceOf[NamedExpression])

  // The result expressions (i.e. result columns)
  def resultExpressions: Seq[NamedExpression]

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("Skyline Distinct", skylineDistinctBoolean)}
       |${ExplainUtils.generateFieldString("Skyline Dimension", skylineDimensionExpressions)}
       |${ExplainUtils.generateFieldString("Results", resultExpressions)}
       |""".stripMargin
  }

  protected def inputAttributes: Seq[Attribute] = {
    child.output
  }

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputExpressions: Seq[NamedExpression] = resultExpressions
}
