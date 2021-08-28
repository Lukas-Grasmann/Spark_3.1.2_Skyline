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

package org.apache.spark.sql.catalyst.expressions.skyline

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.types.DataType

/**
 * Skyline MIN/MAX/DIFF specification
 */
abstract sealed class SkylineMinMaxDiff {
  def sql: String
}

/**
 * Skyline MIN specification
 */
case object SkylineMin extends SkylineMinMaxDiff {
  override def sql: String = "MIN"
}

/**
 * Skyline MAX specification
 */
case object SkylineMax extends SkylineMinMaxDiff {
  override def sql: String = "MAX"
}

/**
 * Skyline DIFF Specification
 */
case object SkylineDiff extends SkylineMinMaxDiff {
  override def sql: String = "DIFF"
}

/**
 * Skyline item options that hold the specifications for a single skyline dimension.
 *
 * @param child child expression (column/dimension)
 * @param minMaxDiff MIN/MAX/DIFF for the given dimension
 */
case class SkylineItemOptions(
  child: Expression,
  minMaxDiff: SkylineMinMaxDiff
) extends Expression with Unevaluable {
  override def toString: String = s"$child ${minMaxDiff.sql}"
  override def sql: String =
    child.sql + " " + minMaxDiff.sql

  override def nullable: Boolean = child.nullable

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = child.dataType

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  override def children: Seq[Expression] = Seq(child)
}

/**
 * Object factory helpers for [[SkylineItemOptions]]
 */
object SkylineItemOptions {
  val MIN = "MIN"
  val MAX = "MAX"
  val DIFF = "DIFF"

  /**
   * Create skyline item options using Boolean, Expression, and String.
   * Conversion from String to [[SkylineMinMaxDiff]] performed here.
   *
   * @param child child expression/column (NOT to be confused with Column() of Spark SQL,
   *              to use a Spark SQL column use column.expression)
   * @param minMaxDiff string specification of "MIN"/"MAX"/"DIFF" (case-insensitive)
   * @return a new object of [[SkylineItemOptions]]
   */
  def createSkylineItemOptions(
    child: Expression,
    minMaxDiff: String
  ): SkylineItemOptions = {
    SkylineItemOptions(
      child,
      minMaxDiff.toUpperCase(Locale.ROOT) match {
        case MIN => SkylineMin
        case MAX => SkylineMax
        case DIFF => SkylineDiff
        case _ => SkylineMin      // fail silently; assume minimization as default
      }
    )
  }

  /**
   * Create skyline item options using Boolean, Expression, and [[SkylineMinMaxDiff]].
   *
   * @param child child expression/column (NOT to be confused with Column() of Spark SQL,
   *              to use a Spark SQL column use column.expression)
   * @param minMaxDiff skyline MIN/MAX/DIFF specification
   * @return a new object of [[SkylineItemOptions]]
   */
  def createSkylineItemOptions(
  child: Expression,
  minMaxDiff: SkylineMinMaxDiff
  ): SkylineItemOptions = {
    SkylineItemOptions(
      child,
      minMaxDiff
    )
  }
}
