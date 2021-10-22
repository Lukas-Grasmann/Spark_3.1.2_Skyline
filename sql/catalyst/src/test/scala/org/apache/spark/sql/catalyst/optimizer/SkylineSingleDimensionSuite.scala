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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, Analyzer}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Max, Min}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, IsNotNull, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDiff, SkylineDimension, SkylineMax, SkylineMin}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, Filter, GlobalLimit, LocalLimit, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class SkylineSingleDimensionSuite extends AnalysisTest {
  val analyzer: Analyzer = getAnalyzer

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SkylineBasic", FixedPoint(1),
      RemoveSingleDimensionalSkylines) :: Nil
  }

  val testRelation: LocalRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("transform single dimensional non-distinct MIN skyline") {
    val query = testRelation.skyline(
      SkylineDimension('a.expr, SkylineMin)
    )
    val optimized = Optimize.execute(query.analyze)

    // CORRECT ANSWER check which does not work in this case as the sub-queries have different
    // aliases. They are semantically equivalent which is not detectable by Spark.
    // Manual checks required.
    /* val correctAnswer = testRelation.where(
        'a === ScalarSubquery(testRelation.select('a).groupBy()(min('a).as("min")))
        && IsNotNull('a)
    ).analyze
    comparePlans(optimized, correctAnswer) */

    // Use custom matching instead
    checkSingleDimensionalMinMaxTransformation(optimized, min = true, distinct = false)
  }

  test("transform single dimensional distinct MIN skyline") {
    val query = testRelation.skylineDistinct(
      SkylineDimension('a.expr, SkylineMin)
    )
    val optimized = Optimize.execute(query.analyze)

    // CORRECT ANSWER check which does not work in this case as the sub-queries have different
    // aliases. They are semantically equivalent which is not detectable by Spark.
    // Manual checks required.
    /* val correctAnswer = testRelation.where(
      'a === ScalarSubquery(testRelation.select('a).groupBy()(min('a).as("min")))
        && IsNotNull('a)
    ).limit(1).analyze
    comparePlans(optimized, correctAnswer) */

    // Use custom matching instead
    checkSingleDimensionalMinMaxTransformation(optimized, min = true, distinct = true)
  }

  test("transform single dimensional non-distinct MAX skyline") {
    val query = testRelation.skyline(
      SkylineDimension('a.expr, SkylineMax)
    )
    val optimized = Optimize.execute(query.analyze)

    // CORRECT ANSWER check which does not work in this case as the sub-queries have different
    // aliases. They are semantically equivalent which is not detectable by Spark.
    // Manual checks required.
    /* val correctAnswer = testRelation.where(
      'a === ScalarSubquery(testRelation.select('a).groupBy()(max('a).as("max")))
        && IsNotNull('a)
    ).analyze
    comparePlans(optimized, correctAnswer) */

    // Use custom matching instead
    checkSingleDimensionalMinMaxTransformation(optimized, min = false, distinct = false)
  }

  test("transform single dimensional distinct MAX skyline") {
    val query = testRelation.skylineDistinct(
      SkylineDimension('a.expr, SkylineMax)
    )
    val optimized = Optimize.execute(query.analyze)

    // CORRECT ANSWER check which does not work in this case as the sub-queries have different
    // aliases. They are semantically equivalent which is not detectable by Spark.
    // Manual checks required.
    /* val correctAnswer = testRelation.where(
      'a === ScalarSubquery(testRelation.select('a).groupBy()(max('a).as("max")))
        && IsNotNull('a)
    ).limit(1).analyze
    comparePlans(optimized, correctAnswer) */

    // Use custom matching instead
    checkSingleDimensionalMinMaxTransformation(optimized, min = false, distinct = true)
  }

  test("do NOT transform single dimensional non-distinct DIFF skyline") {
    val query = testRelation.skyline(
      SkylineDimension('a.expr, SkylineDiff)
    )
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.skyline(
      SkylineDimension('a.expr, SkylineDiff)
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("transform single dimensional distinct DIFF skyline") {
    val query = testRelation.skylineDistinct(
      SkylineDimension('a.expr, SkylineDiff)
    )
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Deduplicate(
      'a :: Nil,
      testRelation
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  def checkSingleDimensionalMinMaxTransformation(
    plan: LogicalPlan,
    min: Boolean,
    distinct: Boolean
  ): Unit = {
    var isFailedCheck = false

    if (distinct) {
      plan match {
        case GlobalLimit(Literal(1, IntegerType),
          LocalLimit(Literal(1, IntegerType),
            Filter(
              And(
                IsNotNull(_),
                EqualTo(_,
                  ScalarSubquery(
                    Aggregate(
                      _,
                      Alias(
                        AggregateExpression(
                          minMax : AggregateFunction,
                          _, _, _, _
                        ),
                        _
                      ) :: Nil,
                      Project(_, _: LocalRelation)
                    ),
                    _, _
                  )
                )
              ),
              _: LocalRelation
            )
          )
        ) =>
          minMax match {
            case _: Min if min =>
              isFailedCheck = false
            case _: Max if !min =>
              isFailedCheck = false
            case _ =>
              isFailedCheck = true
          }
        case _ => isFailedCheck = true
      }
    } else {
      plan match {
        case Filter(
          And(
            IsNotNull(_),
            EqualTo(_,
              ScalarSubquery(
                Aggregate(
                  _,
                  Alias(
                    AggregateExpression(
                      minMax : AggregateFunction,
                      _, _, _, _
                    ),
                    _
                  ) :: Nil,
                  Project(_, _: LocalRelation)
                ),
                _, _
              )
            )
          ),
          _: LocalRelation
        ) =>
          minMax match {
            case _: Min if min =>
              isFailedCheck = false
            case _: Max if !min =>
              isFailedCheck = false
            case _ =>
              isFailedCheck = true
          }
        case _ => isFailedCheck = true
      }
    }

    if (isFailedCheck) {
      fail(
        s"""
           | Plan does not display required transformations!
           | Got instead:
           | ${plan.toString}
            """.trim.stripMargin)
    }
  }
}