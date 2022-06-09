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

import scala.colelction.mutable.Map
import scala.collection.mutable.Set
import scala.collection.mutable.Stack

object CustomLogger
{
    // variables and other global state
    val descriptorString: String = "stopTheClock"

    val pushString: String = "push"
    val popString: String = "pop"

    val applyString: String = "Apply"
    val transformString: String = "Transform"
    val matchString: String = "Match"
    val effectiveMatchString: String = "Effective Match"
    val ineffectiveMatchString: String = "Ineffective Match"
    val executeString: String = "Execute"

	val stateTimes: Map[String, Long] = Map[String, Long](
	  applyString -> 0,
	  transformString -> 0,
	  matchString -> 0,
      effectiveMatchString -> 0,
      ineffectiveMatchString -> 0,
	  executeString -> 0
	  )

	val intermediateStateStack: Stack[String] = Stack[String]()
    val intermediateMatchStack: Stack[Long] = Stack[Long]()
	var previousTimeStamp: Long = 0
    
    val allRulesSet: Set[String] = Set[String]()
    val effectiveRulesSet: Set[String] = Set[String]()
    var currentRule: String = ""
	
    // logger method definitions
    def push(state: String): Unit =
	{
        val currentTime: Long = System.nanoTime
        val timeDiff: Long = currentTime - previousTimeStamp
        if (intermediateStateStack.isEmpty)
        {
            intermediateStateStack.push(state)
            previousTimeStamp = currentTime
        }
        else
        {
            stateTimes(stack.top) += timeDiff
            intermediateStateStack.push(state)
            previousTimeStamp = currentTime
        }
	}
	
	def pop(state: String, effective: Boolean = false): Unit =
	{
        val currentTime: Long = System.nanoTime
        val timeDiff: Long = currentTime - previousTimeStamp
        if (intermediateStateStack.isEmpty)
        {
            throw new RuntimeException(s"Empty stack! :'$state'")
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

    //TODO
    def checkMatch(action: String, timeDiff: Long, effective: Boolean): Unit =
    {
        if (action == pushString)
        {
            intermediateMatchStack.push(timeDiff)
        }
        else if (action == popString)
        {
            if (!intermediateMatchStack.isEmpty)
            {
                if (effective)
                {
                    stateTimes(effectiveMatchString) = intermediateMatchStack.top + timeDiff
                    intermediateMatchStack.pop
                }
                else
                {
                    stateTimes(ineffectiveMatchString) = intermediateMatchStack.top + timeDiff
                    intermediateMatchStack.pop
                }
            }
            else
            {
                throw new RuntimeException("The intermediateMatchStack is empty! cannot pop!")
            }
        }
    }
    //TODO
    def logRulesAsSets(ruleName: String, effective: Boolean = false)
    {
        if (effective)
        {
            effectiveRulesSet += ruleName
        }

        allRulesSet += ruleName
    }

	//TODO
	def logApplyTime[F](
        descriptor: String = "",
        context: String = "",
	    log: (String => Unit) =
	    // scalastyle:off
	    println(_)
	    // scalastyle:on
	    )(anonFunc: => F): F =
	{
        push("Apply")
        val anonFuncRet = anonFunc
        pop("Apply",)
        anonFuncRet
	}

	def logTransformTime[F](
	    descriptor: String = "",
	    context: String = "",
	    log: (String => Unit) =
	    // scalastyle:off
	    println(_)
	    // scalastyle:on
	)(anonFunc: => F): F =
	{
        // Entering a transform.
        push("Transform")
        // Looking for rewrites.
        val anonFuncRet = anonFunc
        // Exiting a transform.
        pop("Transform")
        anonFuncRet
	}

	def logMatchTime[F](
	    descriptor: String = "",
	    unAffected: Boolean = false,
	    context: String = "",
	    log: (String => Unit) =
	    // scalastyle:off
	    println(_)
	    // scalastyle:on
	)(anonFunc: => F): F =
	{
        // Entering a match.
        push("Match")
        // Applying the rewrite
        val anonFuncRet = anonFunc
        // Exiting a match.
        pop("Match")
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
        // Entering execution.
        push("Execute")
        // Run execution.
        val anonFuncRet = anonFunc
        // Exit execution.
        pop("Execute")
        anonFuncRet
	}

	//TODO
	def printData(
        printJSON: Boolean = true,
        printHumanReadable: Boolean = false,
        printASTSize: Boolean = false,
        printAllRules: Boolean = false,
        printEffectiveRules: Boolean = false
    ): Unit =
    {
        val applyTime: Long = stateTimes.getOrElse(applyString, -27)
        val transformTime: Long = stateTimes.getOrElse(transformString, -27)
        val matchTime: Long = stateTimes.getOrElse(matchString, -27)
        val effectiveMatchTime: Long = stateTimes.getOrElse(effectiveMatchString, -27)
        val inefffectiveMatchTime: Long = stateTimes.getOrElse(ineffectiveMatchString, -27)
        val executeTime: Long = stateTimes.getOrElse(executeString, -27)
        
        if (printJSON)
        
            // scalastyle:off
            print(s"""
                ------------------------------\n
                {"data":{"applyTime": $applyTime, "transformTime": $transformTime, "matchTime": $matchTime, "effectiveMatchTime": $effectiveMatchTime, "inefffectiveMatchTime: $effectiveMatchTime, "executeTime": $executeTime}}\n
                ------------------------------\n
                """)
            // scalastyle:on
        }
        
        if (printHumanReadable)
        {
            // scalastyle:off
            print(s"""
                ------------------------------\n
                humanreadableformat\n
                ------------------------------\n
                """)
            // scalastyle:on
        }
        
        if (printASTSize)
        {
            // scalastyle:off
            print(s"""
                ------------------------------\n
                astsize\n
                ------------------------------\n
                """)
            // scalastyle:on
        }
        
        if (printAllRulesSet)
        {
            // scalastyle:off
            print(s"""
                ------------------------------\n
                allrules\n
                ------------------------------\n
                """)
            // scalastyle:on
        }

        if (printEffectiveRules)
        {
            // scalastyle:off
            print(s"""
                ------------------------------\n
                effectiverules\n
                ------------------------------\n
                """)
            // scalastyle:on
        }
    }
}
