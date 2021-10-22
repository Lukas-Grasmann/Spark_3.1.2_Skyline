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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDimension, SkylineMax, SkylineMin}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class SkylineBasicSuite extends AnalysisTest{
  val analyzer: Analyzer = getAnalyzer

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SkylineBasic", FixedPoint(1),
      RemoveEmptySkylines,
      RemoveRedundantSkylineDimensions) :: Nil
  }

  val testRelation: LocalRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("remove redundant skyline dimension") {
    val query = testRelation.skyline(
      SkylineDimension( 'a, SkylineMin ),
      SkylineDimension( 'a, SkylineMin ),
      SkylineDimension( 'b, SkylineMin )
    )
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.skyline(
      SkylineDimension( 'a, SkylineMin ),
      SkylineDimension( 'b, SkylineMin ),
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not remove pseudo-redundant (different MIN/MAX/DIFF) skyline dimension") {
    val query = testRelation.skyline(
      SkylineDimension( 'a, SkylineMin ),
      SkylineDimension( 'a, SkylineMax ),
      SkylineDimension( 'b, SkylineMin ),
    )
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.skyline(
      SkylineDimension( 'a, SkylineMin ),
      SkylineDimension( 'a, SkylineMax ),
      SkylineDimension( 'b, SkylineMin ),
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("remove empty skyline") {
    val query = testRelation.skyline().where('a > Literal(1))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.where('a > Literal(1)).analyze

    comparePlans(optimized, correctAnswer)
  }

}
