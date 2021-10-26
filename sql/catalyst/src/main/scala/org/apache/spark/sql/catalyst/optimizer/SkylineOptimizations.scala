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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, IsNotNull, Literal, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDiff, SkylineIsDistinct, SkylineMax, SkylineMin, SkylineOperator}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, Filter, GlobalLimit, Join, LocalLimit, LogicalPlan, Project}
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
 * Uses Scala built-in .distinct() function on the [[SkylineDimension]].
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
              "max")() :: Nil
            // get minimum in all other cases (only minimized skyline remains)
            case _ => Alias(
              Min(skylineDimension.child).toAggregateExpression(),
              "min")() :: Nil
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

/**
 * Optimizer rule for pushing a skyline operator through a join if it can be fully computed before
 * the join.
 *
 * This rule decreases the input size of both the join and the skyline operator potentially
 * yielding significant performance increases if large amounts of data are removed in the skyline
 * step thus making the join step significantly cheaper.
 *
 * We first check whether the skyline dimensions only regard attributes on either the left or the
 * right side of the join. If this is the case, we reconstruct the join such that the skyline is
 * now a child of the join (either left or right side depending on which the dimensions occur).
 * We also take care that the PROJECT which exists between skyline and join is regarded properly
 * and reintroduced after pushing the skyline through.
 */
object PushSkylineThroughJoin extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform pushSkylineThroughJoin

  private val pushSkylineThroughJoin: PartialFunction[LogicalPlan, LogicalPlan] = {
    // match skylines which are computed on joins
    case s @ SkylineOperator(_, _, dimensions,
      p @ Project(_,
        j @ Join(left, right, _, _, _) ) ) =>
          // check if all skyline dimensions can be found on the left side
          // but NOT on the right side
          if (
            dimensions.map(_.child.references.head).map(_.exprId).forall(
              left.output.map(_.exprId).contains )
            && ! dimensions.map(_.child.references.head).map(_.exprId).exists(
              right.output.map(_.exprId).contains )
          ) {
            p.copy(child = j.copy(left = s.copy(child = left)))
          }
          // check if all skyline dimensions can be found on the right side
          // but NOT on the left side
          else if (
            dimensions.map(_.child.references.head).map(_.exprId).forall(
              right.output.map(_.exprId).contains )
            && ! dimensions.map(_.child.references.head).map(_.exprId).exists(
              left.output.map(_.exprId).contains )
          ) {
            p.copy(child = j.copy(right = s.copy(child = right)))
          }
          // return the skyline as-is if the skyline does not involve only a single side of
          // the join but instead regards both sides
          else { s }
  }
}
