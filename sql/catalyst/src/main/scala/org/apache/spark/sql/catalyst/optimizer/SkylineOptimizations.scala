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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineOperator}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Placeholder class to hold skyline optimizations.
 * Class itself is non-instantiable.
 */
final class SkylineOptimizations private { }

/**
 * Optimizer rule for removing skylines without any relevant dimensions.
 * This is caused i.e. by calling df.skyline() using the DataFrame/DataSet API.
 */
object RemoveEmptySkylines extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp removeEmptySkylines

  private val removeEmptySkylines: PartialFunction[LogicalPlan, LogicalPlan] = {
    case SkylineOperator(skylineItems, child) if skylineItems.isEmpty =>
      child
    case s @ SkylineOperator(_, _) => s
  }
}

/**
 * Optimizer rule for removing redundant dimension specifications from the skyline.
 * Uses Scala built-in .distinct() function on the [[SkylineItemOptions]].
 */
object RemoveRedundantSkylineDimensions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform removeRedundantDimensions

  private val removeRedundantDimensions: PartialFunction[LogicalPlan, LogicalPlan] = {
    case SkylineOperator(skylineItems, child) if skylineItems.nonEmpty =>
      SkylineOperator(skylineItems.distinct, child)
    case s @ SkylineOperator(_, _) => s
  }
}
