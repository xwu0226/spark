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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Adds a jar to the current session so it can be used (for UDFs or serdes).
 */
case class AddJarCommand(path: String) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("result", IntegerType, nullable = false) :: Nil)
    schema.toAttributes
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sessionState.addJar(path)
    Seq(Row(0))
  }
}

/**
 * Adds a file to the current session so it can be used.
 */
case class AddFileCommand(path: String) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sparkContext.addFile(path)
    Seq.empty[Row]
  }
}

/**
 * List files
 */
case class ListFiles(files: Seq[String] = Seq.empty[String]) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("result", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sparkContext.listFiles(files).map(Row(_))
  }
}

/**
 * List jars
 */
case class ListJars() extends RunnableCommand {
  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("result", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }
  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sparkContext.listJars().map(Row(_))
  }
}
