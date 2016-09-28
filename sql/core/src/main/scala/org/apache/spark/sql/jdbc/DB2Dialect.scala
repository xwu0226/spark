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

package org.apache.spark.sql.jdbc

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.sql.types.{BooleanType, DataType, StringType, StructType}

private object DB2Dialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:db2")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case BooleanType => Option(JdbcType("CHAR(1)", java.sql.Types.CHAR))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  /**
   * Returns a PreparedStatement that does Merge table (UPDATE/INSERT)
   */
  override def upsertStatement(
     conn: Connection,
     table: String,
     rddSchema: StructType,
     conditionColumns: Array[String] = Array.empty[String]): PreparedStatement = {
    require(conditionColumns.nonEmpty,
      "Upsert option requires column names on which duplicate rows are identified. " +
        "Please specify option(\"condition_columns\", \"c1, c2, ...\")")

    if (!conditionColumns.forall(rddSchema.fieldNames.contains(_))) {
      throw new IllegalArgumentException(
        "Condition columns specified should be a subset of the schema in the input dataset.\n" +
          s"schema: ${rddSchema.fieldNames.mkString(", ")}\n" +
          s"condition_columns: ${conditionColumns.mkString(", ")}")
    }
    val sourceColumns = rddSchema.fields.map(_.name).mkString(", ")
    val onClause = conditionColumns.map(c => s"T.$c= S.$c").mkString(" AND ")
    val updateClause = rddSchema.fields.map(_.name).filterNot(conditionColumns.contains(_)).
      map(c => s"T.$c = S.$c").mkString(", ")

    val insertColumns = rddSchema.fields.map(c => s"T.${c.name}").mkString(", ")
    val insertValues = rddSchema.fields.map(c => s"S.${c.name}").mkString(", ")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    // In the case where condition columns are the whole set of the rddSchema columns
    // and rddSchema columns may be a subset of the target table schema.
    // We need to do nothing for matched rows
    val sql = if (updateClause != null && updateClause.nonEmpty) {
      s"""
         |MERGE INTO $table AS T
         |USING TABLE (VALUES ($placeholders)) AS S ($sourceColumns)
         |ON ($onClause)
         |WHEN MATCHED THEN UPDATE SET $updateClause
         |WHEN NOT MATCHED THEN INSERT ($insertColumns)
         |VALUES($insertValues)
       """.stripMargin
    } else {
      s"""
         |MERGE INTO $table AS T
         |USING TABLE (VALUES ($placeholders)) AS S ($sourceColumns)
         |ON ($onClause)
         |WHEN NOT MATCHED THEN INSERT ($insertColumns)
         |VALUES($insertValues)
         |ELSE IGNORE
       """.stripMargin
    }
    conn.prepareStatement(sql)
  }
}
