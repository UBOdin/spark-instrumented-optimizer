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
  val stateTimes: Map[String, Long] = Map[String, Long](
    "Apply" -> 0,
    "Transform" -> 0,
    "Match" -> 0,
    "Execute" -> 0
    )
  val intermediateStack: Stack[String] = Stack[String]()
  var previousTimeStamp: Long = 0

  def push(state: String): Unit =
  {
    if (intermediateStack.isEmpty)
    {
      intermediateStack.push(state)
      previousTimeStamp = System.nanoTime()
    }
    else
    {
      stateTimes(stack.top) += (System.nanoTime() - previousTimeStamp)
      intermediateStack.push(state)
      previousTimeStamp = System.nanoTime()
    }
  }
  
  def pop(state: String): Unit =
  {
    if (intermediateStack.isEmpty)
    {
      throw new RuntimeException(s"Empty stack! :'$state'")
    }
    else if (intermediateStack.top != state)
    {
      val wrongState: String = intermediateStack.top
      throw new RuntimeException(s"Cannot pop! :'$wrongState' != '$state'")
    }
    else
    {
      stateTimes(intermediateStack.top) += (System.nanoTime() - previousTimeStamp)
      intermediateStack.pop
      previousTimeStamp = System.nanoTime()
    }
  }

  //TODO
  def logApplyTime()
  {

  }
  
  def logTransformTime[F](
  descriptor: String,
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
  descriptor: String,
  unAffected: Boolean,
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
  descriptor: String,
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
  def pushEffectiveRuleCheck(): Unit =
  {

  }

  //TODO
  def popEffectiveRuleCheck(): Unit =
  {

  }
}
