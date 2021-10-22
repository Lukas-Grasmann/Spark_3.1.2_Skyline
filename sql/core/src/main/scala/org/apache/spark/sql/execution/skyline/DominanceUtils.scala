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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDiff, SkylineMax, SkylineMin, SkylineMinMaxDiff}
import org.apache.spark.sql.types.{AbstractDataType, ByteType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}

/**
 * Contains the results of dominance checks
 */
abstract sealed class DominanceResult {}

/**
 * Tuple A dominates tuple B.
 */
case object Domination extends DominanceResult {}

/**
 * Tuple A is dominated by tuple B.
 * I.e., tuple B dominates tuple A.
 */
case object AntiDomination extends DominanceResult {}

/**
 * Tuple A is equal to tuple B.
 * ONLY WITH REGARDS TO THE SKYLINE DIMENSIONS
 */
case object Equality extends DominanceResult {}

/**
 * Tuple A is incomparable to tuple B.
 */
case object Incomparability extends DominanceResult {}

/**
 * A cell CA partially dominates a cell CB (A != B).
 * The point bp_k is the best point in dimension d_k and D is the set of skyline dimensions.
 * CA.bp_k <= CB.bp_k for every 1<=k<=|D|
 * CA.bp_k = CB.bp_k for at least one 1<=k<=|D|
 * CA.bp_k < CB.bp_k for at least one 1<=k<=|D|
 */
case object PartialDomination extends DominanceResult {}

/**
 * A cell CA is partially dominated by a cell CB (A != B).
 * I.e., the cell CB partially dominates the cell CA (see [[PartialDomination]]).
 */
case object PartialAntiDomination extends DominanceResult {}

/**
 * A cell CA totally dominates a cell CB (A != B).
 * The point bp_k is the best point in dimension d_k and D is the set of skyline dimensions.
 * CA.bp_k < CB.bp_k for every 1<=k<=|D|
 */
case object TotalDomination extends DominanceResult {}

/**
 * A cell CA is totally dominated by a cell CB (A != B).
 * I.e., the cell CB totally dominates the cell CA (see [[TotalDomination]]).
 */
case object TotalAntiDomination extends DominanceResult {}

sealed class DominanceUtils private() {}

/**
 * Object singleton that holds the utilities for checking dominance.
 */
object DominanceUtils extends Logging {
  /**
   * Check dominance for two rows ```rowA``` and ```rowB``` of '''the same''' schema.
   *
   * Skyline dimensions are represented as list of ordinals of the column/dimension
   * and a list of [[SkylineMinMaxDiff]].
   * The lists ```ordinals``` and ```minMaxDiff``` must have the same length.
   *
   * The parameter ```skipNullValues``` specifies how null values are handled. If
   * it is ```false``` then all dimensions are considered regardless whether a value might
   * be null or not. If it is ```true``` then all dimensions are skipped where at least
   * one of the values for the dimension in ```rowA``` or ```rowB``` is null.
   *
   * @param rowA       the row representing the first tuple
   * @param rowB       the row representing the second tuple
   * @param schema     the FULL schema of BOTH tuples (NOT only skyline dimensions)
   * @param ordinals   the ordinals/positions of the skyline dimensions/columns
   * @param minMaxDiff the type of skyline (MIN/MAX/DIFF) for each skyline dimension
   * @param skipNullValues  switch indicating whether null values are skipped
   * @return a [[DominanceResult]] that gives the dominance for the tuples
   *         from the "perspective" of ```rowA```
   */
  def checkRowDominance(
    rowA: InternalRow,
    rowB: InternalRow,
    schema: Seq[AbstractDataType],
    ordinals: Seq[Int],
    minMaxDiff: Seq[SkylineMinMaxDiff],
    skipNullValues: Boolean,
    skipMismatchingNulls: Boolean = false
  ): DominanceResult = {
    // require that all input lists have the same length
    require(ordinals.length == minMaxDiff.length)

    // variables that hold the findings about the tuples
    // if a finding holds for ANY dimension it is set to true
    var a_strictly_better = false;  // a strictly better than b for at least one dimension
    var b_strictly_better = false;  // b strictly better than a for at least one dimension
    var a_different_b = false;      // a different to b for at least one DIFF dimension

    if (skipMismatchingNulls) {
      if (ordinals.exists ( ord => rowA.isNullAt(ord) != rowB.isNullAt(ord) ) ) {
        return Incomparability
      }
    }

    // for each skyline dimension (for each ordinal)
    ordinals.zipWithIndex.foreach { f =>
      // get ordinal as well as index in list of skyline dimensions
      val (ord, idx) = (f._1, f._2)

      if (skipNullValues && ( rowA.isNullAt(ord) || rowB.isNullAt(ord) ) ) {
        // skip dimension where at least one is null
      } else {
        // switch according to data type provided
        schema(ord) match {
          case ByteType | ShortType | IntegerType =>
            // Integral Types (excluding Long)
            val (valA, valB) = if (schema(ord) == ByteType) {
              (rowA.getByte(ord).toInt, rowB.getByte(ord).toInt)
            } else if (schema(ord) == ShortType) {
              (rowA.getShort(ord).toInt, rowB.getShort(ord).toInt)
            } else {
              (rowA.getInt(ord), rowB.getInt(ord))
            }

            // check which dimension is better or different
            minMaxDiff(idx) match {
              case _@SkylineMin =>
                if (valA < valB) {
                  a_strictly_better = true
                }
                else if (valB < valA) {
                  b_strictly_better = true
                }
              case _@SkylineMax =>
                if (valA > valB) {
                  a_strictly_better = true
                }
                else if (valB > valA) {
                  b_strictly_better = true
                }
              case _@SkylineDiff =>
                if (valA != valB) {
                  a_different_b = true
                }
            }
          case LongType =>
            // Long Type
            val (valA, valB) = (rowA.getLong(ord), rowB.getLong(ord))

            // check which dimension is better or different
            minMaxDiff(idx) match {
              case _@SkylineMin =>
                if (valA < valB) {
                  a_strictly_better = true
                }
                else if (valB < valA) {
                  b_strictly_better = true
                }
              case _@SkylineMax =>
                if (valA > valB) {
                  a_strictly_better = true
                }
                else if (valB > valA) {
                  b_strictly_better = true
                }
              case _@SkylineDiff =>
                if (valA != valB) {
                  a_different_b = true
                }
            }
          case FloatType | DoubleType =>
            // Floating Point Types
            val (valA, valB) = if (schema(ord) == FloatType) {
              (rowA.getFloat(ord).toDouble, rowB.getFloat(ord).toDouble)
            } else {
              (rowA.getDouble(ord), rowB.getDouble(ord))
            }

            // check which dimension is better or different
            minMaxDiff(idx) match {
              case _@SkylineMin =>
                if (valA < valB) {
                  a_strictly_better = true
                }
                else if (valB < valA) {
                  b_strictly_better = true
                }
              case _@SkylineMax =>
                if (valA > valB) {
                  a_strictly_better = true
                }
                else if (valB > valA) {
                  b_strictly_better = true
                }
              case _@SkylineDiff =>
                if (valA != valB) {
                  a_different_b = true
                }
            }
          case t@DecimalType =>
            // Decimal Types (max precision)
            val (valA, valB) =
              (rowA.getDecimal(ord, t.MAX_PRECISION, t.MAX_SCALE),
                rowB.getDecimal(ord, t.MAX_PRECISION, t.MAX_SCALE))

            // check which dimension is better or different
            minMaxDiff(idx) match {
              case _@SkylineMin =>
                if (valA < valB) {
                  a_strictly_better = true
                }
                else if (valB < valA) {
                  b_strictly_better = true
                }
              case _@SkylineMax =>
                if (valA > valB) {
                  a_strictly_better = true
                }
                else if (valB > valA) {
                  b_strictly_better = true
                }
              case _@SkylineDiff =>
                if (valA != valB) {
                  a_different_b = true
                }
            }
        }
      }
    }

    // check dominance based on the flags set
    if (a_different_b) {
      // if ANY dimension with DIFF is different then the results are incomparable
      Incomparability
    } else if (a_strictly_better && !b_strictly_better) {
      // if row a is strictly better than row b for at least one dimension row b
      // is NOT strictly better than row a for ALL dimensions then row a dominates
      // row b
      Domination
    } else if (b_strictly_better && !a_strictly_better) {
      // if row b is strictly better than row a for at least one dimension row a
      // is NOT strictly better than row b for ALL dimensions then row b dominates
      // row a, i.e., row a is dominated by row b (anti-domination)
      AntiDomination
    } else if (a_strictly_better && b_strictly_better) {
      // row a is strictly better than row b in at list one dimension but also
      // vice versa then the tuples are incomparable
      Incomparability
    } else {
      // if no dimension is strictly better in any dimension AND also not different
      // for any DIFF dimension then the tuples are equal with regards to the skyline
      // dimensions
      Equality
    }
  }
}
