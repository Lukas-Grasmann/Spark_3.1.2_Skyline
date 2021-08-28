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

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}


/**
 * Skyline distinctiveness specification
 */
abstract sealed class SkylineDistinct {
  def distinct: Boolean
  def sql: String
}

/**
 * Skyline DISTINCT specification
 */
case object SkylineIsDistinct extends SkylineDistinct {
  def distinct: Boolean = true
  def sql: String = "DISTINCT"
}

/**
 * Skyline non-distinct (DISTINCT not set) specification
 */
case object SkylineIsNotDistinct extends SkylineDistinct {
  def distinct: Boolean = false
  def sql: String = ""
}

/**
 * Class that contains a skyline operator for the logical plan.
 *
 * @param distinct Whether or not the skyline is distinct via [[SkylineDistinct]]
 * @param skylineItems Sequence of skyline options (one for each dimension)
 * @param child Child node in logical plan (singular)
 */
case class SkylineOperator(
  distinct: SkylineDistinct,
  skylineItems: Seq[SkylineItemOptions],
  child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def maxRows: Option[Long] = child.maxRows
}

/**
 * Object factory for [[SkylineOperator]]
 */
object SkylineOperator {
  /**
   * Create a new [[SkylineOperator]] using boolean distinct, the skyline dimension items,
   * and the child in the logical plan.
   * Conversion from [[Boolean]] to [[SkylineDistinct]] is performed here.
   *
   * @param distinct Boolean whether or not the items in the skyline are distinct.
   * @param skylineItems Sequence of [[SkylineItemOptions]]
   * @param child child logical plan node ([[LogicalPlan]])
   * @return a new object of [[SkylineOperator]]
   */
  def createSkylineOperator(
    distinct: Boolean,
    skylineItems: Seq[SkylineItemOptions],
    child: LogicalPlan
  ): SkylineOperator = {
    SkylineOperator(
      if (distinct) {
        SkylineIsDistinct
      } else {
        SkylineIsNotDistinct
      },
      skylineItems,
      child
    )
  }

  /**
   * Create a new [[SkylineOperator]] using case class distinct, the skyline dimension items,
   * and the child in the logical plan.
   * Not conversion performed here. Equivalent to calling the regular constructor.
   *
   * @param distinct Boolean whether or not the items in the skyline are distinct.
   * @param skylineItems Sequence of [[SkylineItemOptions]]
   * @param child child logical plan node ([[LogicalPlan]])
   * @return a new object of [[SkylineOperator]]
   */
  def createSkylineOperator(
    distinct: SkylineDistinct,
    skylineItems: Seq[SkylineItemOptions],
    child: LogicalPlan
  ): SkylineOperator = {
    SkylineOperator(
      distinct,
      skylineItems,
      child
    )
  }
}
