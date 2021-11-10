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
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineDimension, SkylineMin}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class SkylinePushThroughSuite extends AnalysisTest {
  val analyzer: Analyzer = getAnalyzer

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SkylineBasic", FixedPoint(1),
      PushSkylineThroughJoin
    ) :: Nil
  }

  val leftTestRelation: LocalRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val rightTestRelation: LocalRelation = LocalRelation('d.int, 'e.int, 'f.int)

  test("push skyline to left side of join") {
    val query = leftTestRelation.join(rightTestRelation, condition = Some('c === 'd))
      .select('a, 'b, 'c, 'd, 'e, 'f)
      .skyline(SkylineDimension('a, SkylineMin), SkylineDimension('b, SkylineMin))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = leftTestRelation
      .skyline(SkylineDimension('a, SkylineMin), SkylineDimension('b, SkylineMin))
      .join(rightTestRelation, condition = Some('c === 'd))
      .select('a, 'b, 'c, 'd, 'e, 'f)

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("push skyline to right side of join") {
    val query = leftTestRelation.join(rightTestRelation, condition = Some('c === 'd))
      .select('a, 'b, 'c, 'd, 'e, 'f)
      .skyline(SkylineDimension('e, SkylineMin), SkylineDimension('f, SkylineMin))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = leftTestRelation
      .join(
        rightTestRelation.skyline(
          SkylineDimension('e, SkylineMin),
          SkylineDimension('f, SkylineMin)),
        condition = Some('c === 'd))
      .select('a, 'b, 'c, 'd, 'e, 'f)

    comparePlans(optimized, correctAnswer.analyze)
  }

  test("do not push skyline to either side of join") {
    val query = leftTestRelation.join(rightTestRelation, condition = Some('c === 'd))
      .select('a, 'b, 'c, 'd, 'e, 'f)
      .skyline(SkylineDimension('a, SkylineMin), SkylineDimension('f, SkylineMin))
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, query.analyze)
  }
}
