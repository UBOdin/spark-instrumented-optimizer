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

package org.apache.spark.sql.catalyst

import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.collection.mutable.Stack
import scala.reflect.runtime.universe._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object CustomLogger
{
    val pushString: String = "push"
    val popString: String = "pop"

    val applyString: String = "Apply"
    val transformString: String = "Transform"
    val transformExpressionString: String = "TransformExpression"
    val matchString: String = "Match"
    val potentialMatchString: String = "Potential Match"
    val effectiveMatchString: String = "Effective Match"
    val ineffectiveMatchString: String = "Ineffective Match"
    val executorString: String = "Executor"

    val stateTimes: Map[String, Long] = Map[String, Long](
        applyString -> 0,
        transformString -> 0,
        transformExpressionString -> 0,
        matchString -> 0,
        effectiveMatchString -> 0,
        potentialMatchString -> 0,
        ineffectiveMatchString -> 0,
        executorString -> 0
    )

    var transformExpressionLogState: Boolean = false
    var tempMatchBucket: Long = 0;
    val intermediateStateStack: Stack[String] = Stack[String]()
    var previousTimeStamp: Long = 0

    val allRulesSet: Set[String] = Set[String]()
    val effectiveRulesSet: Set[String] = Set[String]()
    var currentRule: String = ""

    def push(state: String): Unit =
    {
        val currentTime: Long = System.nanoTime
        val timeDiff: Long = currentTime - previousTimeStamp
        if (intermediateStateStack.isEmpty)
        {
            intermediateStateStack.push(state)
            previousTimeStamp = currentTime
        }
        else if (intermediateStateStack.top == matchString)
        {
            stateTimes(intermediateStateStack.top) += timeDiff
            tempMatchBucket += timeDiff
            intermediateStateStack.push(state)
            previousTimeStamp = currentTime
        }
        else
        {
            stateTimes(intermediateStateStack.top) += timeDiff
            intermediateStateStack.push(state)
            previousTimeStamp = currentTime
        }
    }

    def pop(state: String, checkMatchFlag: Boolean = false): Unit =
    {
        val currentTime: Long = System.nanoTime
        val timeDiff: Long = currentTime - previousTimeStamp
        if (intermediateStateStack.isEmpty)
        {
            throw new RuntimeException(s"Empty stack! :'$state'")
        }
        else if (state == matchString)
        {
            stateTimes(intermediateStateStack.top) += timeDiff
            tempMatchBucket += timeDiff
            intermediateStateStack.pop
            previousTimeStamp = currentTime
        }
        else if (checkMatchFlag && (intermediateStateStack.top == potentialMatchString))
        {
            if (state == effectiveMatchString)
            {
                stateTimes(effectiveMatchString) += tempMatchBucket
                tempMatchBucket = 0
                intermediateStateStack.pop
                previousTimeStamp = currentTime
            }
            else if (state == ineffectiveMatchString)
            {
                stateTimes(ineffectiveMatchString) += tempMatchBucket
                tempMatchBucket = 0
                intermediateStateStack.pop
                previousTimeStamp = currentTime
            }
        }
        else if (intermediateStateStack.top != state)
        {
            val wrongState: String = intermediateStateStack.top
            throw new RuntimeException(s"Cannot pop! :'$wrongState' != '$state'")
        }
        else
        {
            stateTimes(intermediateStateStack.top) += timeDiff
            intermediateStateStack.pop
            previousTimeStamp = currentTime
        }
    }

    def checkMatch(
        action: String = "",
        ruleName: String = "",
        effective: Boolean = false
    ): Unit =
    {
        if (action == pushString)
        {
            push(potentialMatchString)
        }
        else if (action == popString)
        {
            if (ruleName != "")
            {
                logRulesAsSets(ruleName, effective)
            }

            if (effective)
            {
                pop(effectiveMatchString, true)
            }
            else if (!effective)
            {
                pop(ineffectiveMatchString, true)
            }
        }
    }

    def logRulesAsSets(ruleName: String, effective: Boolean = false): Unit =
    {
        if (effective)
        {
            effectiveRulesSet += ruleName
        }

        allRulesSet += ruleName
    }

    def logApplyTime[F](
        descriptor: String = "",
        context: String = "",
        log: (String => Unit) =
        // scalastyle:off println
        println(_)
        // scalastyle:on println
    )(anonFunc: => F): F =
    {
        push(applyString)
        val anonFuncRet = anonFunc
        pop(applyString)
        anonFuncRet
    }

    def logTransformTime[F](
        descriptor: String = "",
        context: String = "",
        log: (String => Unit) =
        // scalastyle:off println
        println(_)
        // scalastyle:on println
    )(anonFunc: => F): F =
    {
        transformExpressionLogState = true
        push(transformString)
        val anonFuncRet = anonFunc
        pop(transformString)
        transformExpressionLogState = false
        anonFuncRet
    }

    def logTransformExpressionTime[F](
        descriptor: String = "",
        context: String = "",
        log: (String => Unit) =
        // scalastyle:off println
        println(_)
        // scalastyle:on println
    )(anonFunc: => F): F =
    {
        if (transformExpressionLogState)
        {
            push(transformExpressionString)
            val anonFuncRet = anonFunc
            pop(transformExpressionString)
            anonFuncRet
        }
        else
        {
            val anonFuncRet = anonFunc
            anonFuncRet
        }
    }

    def logMatchTime[F](
        descriptor: String = "",
        unAffected: Boolean = false,
        context: String = "",
        log: (String => Unit) =
        // scalastyle:off println
        println(_)
        // scalastyle:on println
    )(anonFunc: => F): F =
    {
        push(matchString)
        val anonFuncRet = anonFunc
        pop(matchString)
        anonFuncRet
    }

    def logExecutionTime[F](
        descriptor: String = "",
        context: String = "",
        log: (String => Unit) =
        // scalastyle:off
        println(_)
        // scalastyle:on
    )(anonFunc: => F): F =
    {
        push(executorString)
        val anonFuncRet = anonFunc
        pop(executorString)
        anonFuncRet
    }

    def printData(
        printJSON: Boolean = true,
        printHumanReadable: Boolean = false,
        printAllRules: Boolean = false,
        printEffectiveRules: Boolean = false,
        printASTSizes: Boolean = false,
        optimizedASTSize: Long = 0,
        analyzedASTSize: Long = 0
    ): Unit =
    {
        val applyTime: Long = stateTimes.getOrElse(applyString, -27)
        val transformTime: Long = stateTimes.getOrElse(transformString, -27)
        val transformExpressionTime: Long = stateTimes.getOrElse(transformExpressionString, -27)
        val matchTime: Long = stateTimes.getOrElse(matchString, -27)
        val effectiveMatchTime: Long = stateTimes.getOrElse(effectiveMatchString, -27)
        val ineffectiveMatchTime: Long = stateTimes.getOrElse(ineffectiveMatchString, -27)
        val executorTime: Long = stateTimes.getOrElse(executorString, -27)

        if (printJSON)
        {
            // scalastyle:off
            print(s"""
------------------------------
{"data":{"applyTime": $applyTime, "transformTime": $transformTime, "transformExpressionTime": $transformExpressionTime, "matchTime": $matchTime, "effectiveMatchTime": $effectiveMatchTime, "ineffectiveMatchTime": $ineffectiveMatchTime, "executorTime": $executorTime}}
------------------------------
""")
            // scalastyle:on
        }

        if (printHumanReadable)
        {
            val applyTimeSec: Double = (applyTime / 1000000000.0)
            val transformTimeSec: Double = (transformTime / 1000000000.0)
            val transformExpressionTimeSec: Double = (transformExpressionTime / 1000000000.0)
            val effectiveMatchTimeSec: Double = (effectiveMatchTime / 1000000000.0)
            val ineffectiveMatchTimeSec: Double = (ineffectiveMatchTime / 1000000000.0)
            val matchTimeSec: Double = (matchTime / 1000000000.0)
            val executorTimeSec: Double = (executorTime / 1000000000.0)

            val matchTimeNotAccountedFor: Double = (applyTime - transformTime)
            val matchTimeNotAccountedForSec: Double = (matchTimeNotAccountedFor / 1000000000.0)

            val totalTime: Double = (
                transformTime + matchTime + executorTime + matchTimeNotAccountedFor)
            val totalTimeSec: Double = (totalTime / 1000000000.0)

            // scalastyle:off
            print(s"""
------------------------------
Total time for applying rules: $applyTime ns or $applyTimeSec seconds.


Total time for searching: $transformTime ns or $transformTimeSec seconds.

Total time for expression transformations: $transformExpressionTime ns or $transformExpressionTimeSec seconds.


Time for effective rule matching: $effectiveMatchTime ns or $effectiveMatchTimeSec seconds.

Time for ineffective rule matching: $ineffectiveMatchTime ns or $ineffectiveMatchTimeSec seconds.

Total time for rule matching: $matchTime ns or $matchTimeSec seconds.

Time for rule matching and searching that is not being tracked: $matchTimeNotAccountedFor ns or $matchTimeNotAccountedForSec seconds.


Total time for executor: $executorTime ns or $executorTimeSec seconds.


Total time for everything: $totalTime ns or $totalTimeSec seconds.
------------------------------
""")
            // scalastyle:on
        }

        if (printAllRules)
        {
            // scalastyle:off
            print(s"""
------------------------------
Set of all rules:
$allRulesSet
------------------------------
""")
            // scalastyle:on
        }

        if (printEffectiveRules)
        {
            // scalastyle:off
            print(s"""
------------------------------
Set of effective rules:
$effectiveRulesSet
------------------------------
""")
            // scalastyle:on
        }

        if (printASTSizes)
        {
            // scalastyle:off
            print(s"""
------------------------------
Optimized AST Size: $optimizedASTSize
Analyzed AST Size: $analyzedASTSize
------------------------------
""")
            // scalastyle:on
        }
    }
}
