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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDistinct, SkylineItemOptions}
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
     skylineDimensions: Seq[SkylineItemOptions],
     requiredChildDistributionExpressions: Option[Seq[Expression]],
     child: SparkPlan
  ): SparkPlan = {
    // check whether at least one dimension is nullable
    val dimensionNullable =
      skylineDimensions.map{f => f.child.nullable}.fold(false)((a, b) => a && b)

    if (dimensionNullable) {
      // TODO replace by skyline algorithm that supports null values
      BlockNestedLoopSkylineExec(
        skylineDistinct,
        skylineDimensions,
        requiredChildDistributionExpressions,
        child
      )
    } else {
      BlockNestedLoopSkylineExec(
        skylineDistinct,
        skylineDimensions,
        requiredChildDistributionExpressions,
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
     skylineDimensions: Seq[SkylineItemOptions],
     child: SparkPlan
   ) : Seq[SparkPlan] = {
    val localSkyline = createSkyline(
      skylineDistinct,
      skylineDimensions,
      None,
      child
    )

    val globalSkyline = createSkyline(
      skylineDistinct,
      skylineDimensions,
      Some(Nil),
      localSkyline
    )

    globalSkyline :: Nil
  }
}
