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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, IsNotNull, Literal, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDiff, SkylineIsDistinct, SkylineMax, SkylineMin, SkylineOperator}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, Filter, GlobalLimit, LocalLimit, LogicalPlan, Project}
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
    case SkylineOperator(_, _, skylineItems, child) if skylineItems.isEmpty =>
      child
    case s @ SkylineOperator(_, _, _, _) => s
  }
}

/**
 * Optimizer rule for removing redundant dimension specifications from the skyline.
 * Uses Scala built-in .distinct() function on the SkylineItemOptions.
 */
object RemoveRedundantSkylineDimensions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform removeRedundantDimensions

  private val removeRedundantDimensions: PartialFunction[LogicalPlan, LogicalPlan] = {
    case SkylineOperator(distinct, complete, skylineItems, child) if skylineItems.nonEmpty =>
      SkylineOperator(distinct, complete, skylineItems.distinct, child)
    case s @ SkylineOperator(_, _, _, _) => s
  }
}

/**
 * Optimizer rule for transforming single-dimensional skyline operators to aggregates.
 *
 * We calculate the minimum and maximum value for MIN and MAX respectively and then select
 * the rows where the values match. Since it is single-dimensional and we only select the minimal
 * or maximal values, we also introduce a LIMIT of 1 in case of DISTINCT.
 *
 * In case of DIFF, we perform no action in case of the regular skyline since no MIN or MAX was
 * given and therefore no strictly better values are possible which are required for dominance.
 * If DISTINCT was specified in combination with DIFF then we use DEDUPLICATE to remove duplicates
 * in the respective skyline dimensions.
 * ATTENTION: Note that the DEDUPLICATE must be transformed to an aggregate by using
 * ReplaceDeduplicateWithAggregate. The rule must therefore appear AFTER the deduplication.
 */
object RemoveSingleDimensionalSkylines extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform removeSingleDimensionalSkylines

  private val removeSingleDimensionalSkylines: PartialFunction[LogicalPlan, LogicalPlan] = {
    // in case of a SkylineOperator with
    // - a single skyline dimension WHICH IS
    // - either minimized (MIN) or maximized (MAX)
    case SkylineOperator(distinct, _, skylineItems, child)
      if (
        skylineItems.size == 1
        && (skylineItems.head.minMaxDiff==SkylineMin || skylineItems.head.minMaxDiff==SkylineMax)
      ) =>
        // get the single skyline dimension via head
        val skylineDimension = skylineItems.head

        // construct a projection to the skyline dimension
        val projection = Project(skylineDimension.child.asInstanceOf[NamedExpression] :: Nil, child)
        // construct an aggregate that selects the minimum or maximum value
        val aggregate = Aggregate(
          Nil,  // no grouping
          skylineItems.head.minMaxDiff match {
            // get maximum in case of maximized skyline
            case SkylineMax => Alias(
              Max(skylineDimension.child).toAggregateExpression(),
              "max(" + skylineDimension.child.sql + ")")() :: Nil
            // get minimum in all other cases (only minimized skyline remains)
            case _ => Alias(
              Min(skylineDimension.child).toAggregateExpression(),
              "min(" + skylineDimension.child.sql + ")")() :: Nil
          },
          projection  // use projection to skyline dimension as input for aggregate
        )
        // encapsulate aggregate in scalar (single result) sub-query
        val subQuery = ScalarSubquery(aggregate)
        // find only tuples which have teh same value as the result of the sub-query
        // we also disregard null values for this
        val filter = Filter(
          And(
            IsNotNull(skylineDimension.child),
            EqualTo(skylineDimension.child, subQuery)
          ),
          child
        )

      if (distinct == SkylineIsDistinct) {
        // if distinct skyline we only take the single (arbitrarily chosen) top result
        GlobalLimit(Literal(1), LocalLimit(Literal(1), filter))
      } else {
        // otherwise we return all tuples with the given value
        filter
      }
    // in case of a skyline operator with a single dimension AND
    // the dimension is of the DIFF skyline type AND
    // the skyline is distinct
    case SkylineOperator(_@SkylineIsDistinct, _, skylineItems, child)
      if skylineItems.size == 1 && skylineItems.head.minMaxDiff == SkylineDiff =>
        // since the values should be distinct, we eliminate duplicates using Deduplicate
        // ATTENTION: Deduplicate must be transformed to aggregate by optimizations
        Deduplicate(skylineItems.head.child.references.toSeq, child)
  }
}
