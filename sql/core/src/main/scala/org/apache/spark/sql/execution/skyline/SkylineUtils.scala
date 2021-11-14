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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNull}
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineComplete, SkylineDimension, SkylineDistinct, SkylineForceBNL, SkylineIsComplete}
import org.apache.spark.sql.execution.SparkPlan

/**
 * Utility functions used by the query planner to convert the plan to the new skyline code plan.
 */
object SkylineUtils extends Logging {
  /**
   * Function for choosing the correct skyline algorithm for the operator.
   *
   * @param skylineDistinct whether the skyline is distinct
   * @param skylineDimensions the skyline dimensions used by the operator
   * @param requiredChildDistributionExpressions the distribution of the children
   * @param child child node in plan (used as input)
   * @return
   */
  private def createSkyline(
     skylineDistinct: SkylineDistinct,
     skylineDimensions: Seq[SkylineDimension],
     requiredChildDistributionExpressions: Option[Seq[Expression]],
     globalSkyline: Boolean,
     incompleteSkyline: Boolean,
     child: SparkPlan
  ): SparkPlan = {

   if (incompleteSkyline && globalSkyline) {
      IncompleteSkylineExec(
        skylineDistinct,
        skylineDimensions,
        child
      )
    } else if (incompleteSkyline) {
     BlockNestedLoopSkylineExec(
       skylineDistinct,
       skylineDimensions,
       Some(skylineDimensions.map { f => IsNull(f.child) }),
       isIncompleteSkyline = true,
       child
     )
   } else if (globalSkyline) {
      BlockNestedLoopSkylineExec(
        skylineDistinct,
        skylineDimensions,
        Some(Nil),
        isIncompleteSkyline = false,
        child
      )
    } else {
     BlockNestedLoopSkylineExec(
       skylineDistinct,
       skylineDimensions,
       requiredChildDistributionExpressions,
       isIncompleteSkyline = false,
       child
     )
   }
  }

  /**
   * Automatically construct a full physical plan to compute the skylines.
   *
   * @param skylineDistinct whether or not the skyline is distinct
   * @param skylineDimensions the dimensions of the skyline
   * @param child the child physical plan; usually uses planLater()
   * @return physical plan including nodes to compute the skyline with an appropriate algorithm
   */
  def planSkyline(
     skylineDistinct: SkylineDistinct,
     skylineComplete: SkylineComplete,
     skylineDimensions: Seq[SkylineDimension],
     child: SparkPlan
   ) : Seq[SparkPlan] = {

    if (skylineComplete == SkylineForceBNL) {
      BlockNestedLoopSkylineExec(
        skylineDistinct,
        skylineDimensions,
        Some(Nil),
        isIncompleteSkyline = false,
        child
      )
    }

    // check whether at least one dimension is nullable
    val dimensionNullable = skylineComplete match {
      // treat all dimensions as if they were not nullable
      case SkylineIsComplete => false
      // determine whether to treat the dimensions as nullable by checking whether the dimension
      // is marked as nullable in Spark
      case _ => skylineDimensions.exists(_.child.nullable)
    }

    val localSkyline: SparkPlan = createSkyline(
      skylineDistinct,
      skylineDimensions,
      None,
      globalSkyline = false,
      dimensionNullable,
      child
    )

    val globalSkyline = createSkyline(
      skylineDistinct,
      skylineDimensions,
      Some(Nil),
      globalSkyline = true,
      dimensionNullable,
      localSkyline
    )

    globalSkyline :: Nil
  }
}
