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
import org.apache.spark.sql.catalyst.expressions.{IsNull}
import org.apache.spark.sql.catalyst.expressions.skyline.SkylineIsNotDistinct
import org.apache.spark.sql.execution.skyline.{BlockNestedLoopSkylineExec, IncompleteSkylineExec}
import org.apache.spark.sql.test.SharedSparkSession

class SkylineIncompleteSuite extends SparkPlanTest with SharedSparkSession {
  import testImplicits.{localSeqToDatasetHolder, newProductEncoder}

  test("basic min skyline using incomplete distributed skylines") {
    val input = Seq(
      ("A", Some(1), Some(2.0)),
      ("B", None, Some(4.0)),
      ("C", Some(4), None)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => IncompleteSkylineExec(
        SkylineIsNotDistinct,
        'b.smin :: 'c.smin :: Nil,
        BlockNestedLoopSkylineExec(
          SkylineIsNotDistinct,
          'b.smin :: 'c.smin :: Nil,
          requiredChildDistributionExpressions =
            Some(IsNull('b).expr :: IsNull('c).expr :: Nil),
          isIncompleteSkyline = true,
          child
        )
      ),
      Seq(("A", 1, 2.0)).map(Row.fromTuple))
  }

  test("extended min skyline using incomplete distributed skylines") {
    val input = Seq(
      ("A", None, Some(4.0), None),
      ("B", None, Some(5.0), None),
      ("C", Some(4), None, None),
      ("D", Some(5), None, None),
      ("E", None, None, Some(4.0)),
      ("F", None, None, Some(3.0))
    )

    checkAnswer(
      input.toDF("a", "b", "c", "d"),
      (child: SparkPlan) => IncompleteSkylineExec(
        SkylineIsNotDistinct,
        'b.smin :: 'c.smin :: 'd.smin :: Nil,
        BlockNestedLoopSkylineExec(
          SkylineIsNotDistinct,
          'b.smin :: 'c.smin :: 'd.smin :: Nil,
          requiredChildDistributionExpressions =
            Some(
              IsNull('b).expr :: IsNull('c).expr :: IsNull('d).expr :: Nil),
          isIncompleteSkyline = true,
          child
        )
      ),
      // beware that while we use Some() and None() in the input,
      // the result will contain the values without Some() and all None() will be replaced by null
      Seq(("A", null, 4.0, null), ("C", 4, null, null), ("F", null, null, 3.0)).map(Row.fromTuple))
  }

  test("eliminate cyclic dominance in incomplete skylines") {
    val input = Seq(
      ("A", Some(1), None, Some(10)),
      ("B", Some(3), Some(2), None),
      ("C", None, Some(5), Some(3))
    )

    checkAnswer(
      input.toDF("a", "b", "c", "d"),
      (child: SparkPlan) => IncompleteSkylineExec(
        SkylineIsNotDistinct,
        'b.smin :: 'c.smin :: 'd.smin :: Nil,
        BlockNestedLoopSkylineExec(
          SkylineIsNotDistinct,
          'b.smin :: 'c.smin :: 'd.smin :: Nil,
          requiredChildDistributionExpressions =
            Some(IsNull('b).expr :: IsNull('c).expr :: IsNull('d).expr :: Nil),
          isIncompleteSkyline = true,
          child
        )
      ),
      Seq().map(Row.fromTuple))
  }
}
