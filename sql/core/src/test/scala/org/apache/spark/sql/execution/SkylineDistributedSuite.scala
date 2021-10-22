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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.skyline.{SkylineIsDistinct, SkylineIsNotDistinct}
import org.apache.spark.sql.execution.skyline.BlockNestedLoopSkylineExec
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.exceptions.TestFailedException

class SkylineDistributedSuite extends SparkPlanTest with SharedSparkSession {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  test("basic min skyline using BlockNestedLoop") {
    val input = Seq(
      ("A", 1, 2.0),
      ("B", 3, 4.0),
      ("C", 4, 3.0)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => BlockNestedLoopSkylineExec(
        SkylineIsNotDistinct,
        'b.smin :: 'c.smin :: Nil,
        requiredChildDistributionExpressions = Some(Nil),
        isIncompleteSkyline = false,
        BlockNestedLoopSkylineExec(
          SkylineIsNotDistinct,
          'b.smin :: 'c.smin :: Nil,
          requiredChildDistributionExpressions = None,
          isIncompleteSkyline = false,
          child
        )
      ),
      Seq(("A", 1, 2.0)).map(Row.fromTuple))
  }

  test("basic max skyline using BlockNestedLoop") {
    val input = Seq(
      ("A", 1, 2.0),
      ("B", 3, 4.0),
      ("C", 4, 3.0)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => BlockNestedLoopSkylineExec(
        SkylineIsNotDistinct,
        'b.smax :: 'c.smax :: Nil,
        requiredChildDistributionExpressions = Some(Nil),
        isIncompleteSkyline = false,
        BlockNestedLoopSkylineExec(
          SkylineIsNotDistinct,
          'b.smax :: 'c.smax :: Nil,
          requiredChildDistributionExpressions = None,
          isIncompleteSkyline = false,
          child
        )
      ),
      Seq(("B", 3, 4.0), ("C", 4, 3.0)).map(Row.fromTuple))
  }

  test("basic diff skyline using BlockNestedLoop") {
    val input = Seq(
      ("A", 1, 2.0, 1),
      ("B", 3, 4.0, 2),
      ("C", 4, 3.0, 3)
    )

    checkAnswer(
      input.toDF("a", "b", "c", "d"),
      (child: SparkPlan) => BlockNestedLoopSkylineExec(
        SkylineIsNotDistinct,
        'b.smax :: 'c.smax :: 'd.sdiff :: Nil,
        requiredChildDistributionExpressions = Some(Nil),
        isIncompleteSkyline = false,
        BlockNestedLoopSkylineExec(
          SkylineIsNotDistinct,
          'b.smax :: 'c.smax :: 'd.sdiff :: Nil,
          requiredChildDistributionExpressions = None,
          isIncompleteSkyline = false,
          child
        )
      ),
      Seq(("A", 1, 2.0, 1), ("B", 3, 4.0, 2), ("C", 4, 3.0, 3)).map(Row.fromTuple))
  }

  test("non-distinct skyline using BlockNestedLoop") {
    val input = Seq(
      ("A", 1, 2.0),
      ("B", 1, 2.0),
      ("C", 4, 3.0)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => BlockNestedLoopSkylineExec(
        SkylineIsNotDistinct,
        'b.smin :: 'c.smin :: Nil,
        requiredChildDistributionExpressions = Some(Nil),
        isIncompleteSkyline = false,
        BlockNestedLoopSkylineExec(
          SkylineIsNotDistinct,
          'b.smin :: 'c.smin :: Nil,
          requiredChildDistributionExpressions = None,
          isIncompleteSkyline = false,
          child
        )
      ),
      Seq(("A", 1, 2.0), ("B", 1, 2.0)).map(Row.fromTuple))
  }

  test("distinct skyline using BlockNestedLoop") {
    val input = Seq(
      ("A", 1, 2.0),
      ("B", 1, 2.0),
      ("C", 4, 3.0)
    )

    try {
      checkAnswer(
        input.toDF("a", "b", "c"),
        (child: SparkPlan) => BlockNestedLoopSkylineExec(
          SkylineIsDistinct,
          'b.smin :: 'c.smin :: Nil,
          requiredChildDistributionExpressions = Some(Nil),
          isIncompleteSkyline = false,
          BlockNestedLoopSkylineExec(
            SkylineIsDistinct,
            'b.smin :: 'c.smin :: Nil,
            requiredChildDistributionExpressions = None,
            isIncompleteSkyline = false,
            child
          )
        ),
        Seq(("B", 1, 2.0)).map(Row.fromTuple)) // MAY DEVIATE; CHECK TEST CASE IN CATCH
    } catch {
      case _: TestFailedException =>
        checkAnswer(
          input.toDF("a", "b", "c"),
          (child: SparkPlan) => BlockNestedLoopSkylineExec(
            SkylineIsDistinct,
            'b.smin :: 'c.smin :: Nil,
            requiredChildDistributionExpressions = Some(Nil),
            isIncompleteSkyline = false,
            BlockNestedLoopSkylineExec(
              SkylineIsDistinct,
              'b.smin :: 'c.smin :: Nil,
              requiredChildDistributionExpressions = None,
              isIncompleteSkyline = false,
              child
            )
          ),
          Seq(("A", 1, 2.0)).map(Row.fromTuple))
    }
  }
}
