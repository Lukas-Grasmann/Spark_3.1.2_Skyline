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
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDimension, SkylineDistinct}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{AliasAwareOutputPartitioning, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Physical plan node for computing a skyline via the Block-Nested-Loop Algorithm.
 *
 * <p>
 * This algorithm can be used for both local and global skylines and has the same output attributes
 * and input attributes. Local and global skylines are distinguished via
 * [[requiredChildDistributionExpressions]] which distributes according to the following cases:
 *
 * <ul>
 *   <li>empty expression list: all tuples to a single node</li>
 *   <li>list of expressions: clustering according to expression</li>
 *   <li>None: unspecified clustering (gives clustering authority to Spark)</li>
 * </ul>
 * </p>
 *
 * @param skylineDistinct whether or not the results should be distinct
 *                        with regards to the skyline dimensions
 * @param skylineDimensions list of skyline dimensions as [[SkylineDimension]]
 * @param requiredChildDistributionExpressions expressions for the distribution (see above)
 * @param isIncompleteSkyline specifies whether the skyline is incomplete
 * @param child child node in plan which produces the input for the skyline operator
 */
case class BlockNestedLoopSkylineExec(
  skylineDistinct: SkylineDistinct,
  skylineDimensions: Seq[SkylineDimension],
  requiredChildDistributionExpressions: Option[Seq[Expression]],
  isIncompleteSkyline: Boolean,
  child: SparkPlan
) extends BaseSkylineExec with AliasAwareOutputPartitioning with Logging {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def resultExpressions: Seq[NamedExpression] = child.output

  override def requiredChildDistribution: Seq[Distribution] = {
    // chose distribution according to the distribution expression
    // this is comparable to the distribution in Aggregate
    requiredChildDistributionExpressions match {
      // if empty list of expressions then everything goes to one node/partition
      case Some(expression) if expression.isEmpty => AllTuples :: Nil
      // if expressions where specified then cluster according to expressions
      case Some(expressions) => ClusteredDistribution(expressions) :: Nil
      // if None was specified the distribution is unspecified (i.e. automatically chosen)
      case None => UnspecifiedDistribution :: Nil
    }
  }

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
      val blockNestedLoopWindow = new mutable.ArrayBuffer[InternalRow]()

      // for each row in the partition(-iterator)
      partitionIter.foreach { row =>
        // flag that checks whether the current row is dominated
        var isDominated = false
        // flag for breaking if the tuple is already dominated
        var breakWindowCheck = false

        // store skyline tuples to be removed
        val dominatedSkylineTuples = new mutable.ArrayBuffer[InternalRow]()

        // emulate foreach loop using an enumerator
        // additionally break loop if current row is itself dominated
        val iter = blockNestedLoopWindow.iterator
        while (iter.hasNext && !breakWindowCheck) {
          val windowRow = iter.next()

          // check dominance for row
          // use [[DominanceUtils]] for converting the row and actually checking dominance
          val dominationResult = DominanceUtils.checkRowDominance(
            row,
            windowRow,
            childOutputSchema,
            skylineDimensionOrdinals,
            skylineDimensions.map { f => f.minMaxDiff },
            skipNullValues = isIncompleteSkyline,
            skipMismatchingNulls = isIncompleteSkyline
          )

          // check domination result and chose action accordingly
          dominationResult match {
            case Domination =>
              // if the current row dominates another row the row is removed
              dominatedSkylineTuples.append(windowRow.copy())
            case AntiDomination =>
              // if the row is itself dominated we do not add it by setting isDominated and
              // we can stop checking the rest of the window
              isDominated = true
              breakWindowCheck = true
            case Equality =>
              // when equal in every dimension, we can proceed as if it was dominated for skylines
              // where DISTINCT was specified
              // otherwise we can just add it since an equal tuple is already in the skyline
              if (skylineDistinctBoolean) { isDominated = true }
              breakWindowCheck = true
            case Incomparability | _ =>
              // NO ACTION
          }
        }

        // only add to window if NOT dominated
        if (!isDominated) {
          // only if the current tuple was NOT dominated AND
          // there exist tuples which are dominated by the current tuple
          if (dominatedSkylineTuples.nonEmpty) {
            blockNestedLoopWindow --= dominatedSkylineTuples
          }

          // add current (non-dominated) tuple to skyline
          blockNestedLoopWindow.append(row.copy())
        }

        // #############################################################
        // # NOTE REGARDING ABOVE ELIMINATIONS FROM THE SKYLINE WINDOW #
        // #############################################################
        //
        // COMPLETE DATASETS:
        // If a tuple is dominated by another tuple already in the block-nested-loop window
        // (which is a valid skyline of part of the dataset), it cannot dominate any tuple in
        // said window due to the transitivity property.
        //
        // INCOMPLETE DATASET:
        // The same requirement as for complete datasets is ensured by skipping dimensionally
        // incomparable tuples in the incomplete case (i.e. tuples that have missing values
        // in different skyline dimensions).
        // Thereby, the result of the algorithm contains correct (local skyline) results even if
        // multiple clusters grouped by different missing values get sent to the same worker.
        // Note that this means that such incomparable values can therefore also end up together
        // in the result set of the worker.
        // A WORKERS RESULT MAY THEREFORE CONTAIN DATA FROM MULTIPLE CLUSTERS BUT EACH CLUSTER IS
        // ASSIGNED TO A SINGLE WORKER IN FULL.
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
