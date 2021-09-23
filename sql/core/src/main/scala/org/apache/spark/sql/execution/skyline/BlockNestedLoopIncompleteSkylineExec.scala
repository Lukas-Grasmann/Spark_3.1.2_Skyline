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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDistinct, SkylineItemOptions}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution}
import org.apache.spark.sql.execution.{AliasAwareOutputPartitioning, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Physical plan for computing an incomplete skyline via the Block-Nested-Loop algorithm.
 *
 * This algorithm is used to compute an incomplete skyline i.e. skylines where some values might
 * be missing (i.e. null). Since the null values impact the computation of the dominance via
 * cyclic dominance and the resulting loss of transitivity, we need to modify the algorithm.
 * A version that does not consider null values can be found in [[BlockNestedLoopSkylineExec]].
 *
 * <p>
 *  This algorithm is used exclusively for computing dominance for the global incomplete skyline
 * operator. It therefore has a child distribution of [[AllTuples]].
 * </p>
 *
 * @param skylineDistinct whether or not the results should be distinct
 *                        with regards to the skyline dimensions
 * @param skylineDimensions list of skyline dimensions as [[SkylineItemOptions]]
 * @param child child node in plan which produces the input for the skyline operator
 */
case class BlockNestedLoopIncompleteSkylineExec(
 skylineDistinct: SkylineDistinct,
 skylineDimensions: Seq[SkylineItemOptions],
 child: SparkPlan
) extends BaseSkylineExec with AliasAwareOutputPartitioning with Logging {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def resultExpressions: Seq[NamedExpression] = child.output

  override def requiredChildDistribution: Seq[Distribution] = AllTuples :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    // load metrics
    val numOutputRows = longMetric("numOutputRows")

    // precalculate output schema as list of [[DataType]]
    val childOutputSchema = child.output.map { f => f.dataType }
    // precalculate ordinals for each skyline dimension
    val skylineDimensionOrdinals = skylineDimensions.map { option =>
      // get ordinal of the skyline dimension
      child.output.map{ attr => attr.exprId }.indexOf(option.child.references.head.exprId)
    }


    // execute child and for each partition (corresponding iterator) calculate the skyline
    // in case of [[AllTuples]] there is only exactly one partition (on one node)
    child.execute().mapPartitionsInternal { partitionIter =>
      // window for holding current skyline
      val blockNestedLoopWindow = new mutable.HashSet[InternalRow]()

      // for each row in the partition(-iterator)
      partitionIter.foreach { row =>
        // flag that checks whether the current row is dominated
        var isDominated = false;

        // emulate foreach loop using an enumerator
        // additionally break loop if current row is itself dominated
        val iter = blockNestedLoopWindow.iterator
        while (iter.hasNext) {
          val windowRow = iter.next()

          // check dominance for row
          // use [[DominanceUtils]] for converting the row and actually checking dominance
          val dominationResult = DominanceUtils.checkRowDominance(
            row,
            windowRow,
            childOutputSchema,
            skylineDimensionOrdinals,
            skylineDimensions.map { f => f.minMaxDiff },
            skipNullValues = true
          )

          // check domination result and chose action accordingly
          dominationResult match {
            case Domination =>
              // if the current row dominates another row the row is removed
              blockNestedLoopWindow.remove(windowRow)
            case AntiDomination =>
              // if the row is itself dominated we do not add it by setting isDominated
              isDominated = true
            case Equality | Incomparability | _ =>
              // NO ACTION
              // note that all [[Equality]] results have no effect on the skyline
              // all true duplicates were already removed in the local skyline
              // this is due to the fact that true duplicates also have the same null values
              // and are therefore in the same cluster
          }
        }

        // only add to window if NOT dominated
        if (!isDominated) {
          blockNestedLoopWindow += row.copy()
        }
      }

      // the number of rows after the final iteration is equal to the number of output tuples
      // we consider here that there may be multiple partitions
      numOutputRows += blockNestedLoopWindow.size
      // return the window contents as result using an iterator
      // the handling of the partitioning is done by mapPartitionsInternal
      blockNestedLoopWindow.toIterator
    }
  }
}
