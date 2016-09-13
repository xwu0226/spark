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
     conditionColumns: Seq[String] = Seq.empty[String]): PreparedStatement = {
    require(conditionColumns.nonEmpty,
      "Upsert mode requires column names on which duplicate rows are identified.")

    val sourceColumns = rddSchema.fields.map { x =>
      s"${this.quoteIdentifier(x.name)}"
    }.mkString(", ")

    val onClause = conditionColumns.map { c =>
      val quotedColumnName = this.quoteIdentifier(c)
      s"T.${quotedColumnName} = S.${quotedColumnName}"
    }.mkString(" AND ")

    val updateClause = rddSchema.fields.map(_.name).filterNot(conditionColumns.contains(_)).
      map { x =>
        val quotedColumnName = this.quoteIdentifier(x)
        s"T.${quotedColumnName} = S.${quotedColumnName}"
      }.mkString(", ")

    val insertColumns = rddSchema.fields.map { x =>
      s"T.${this.quoteIdentifier(x.name)}"
    }.mkString(", ")

    val insertValues = rddSchema.fields.map { x =>
      s"S.${this.quoteIdentifier(x.name)}"
    }.mkString(", ")

    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql =
      s"""
         |MERGE INTO $table AS T
         |USING TABLE(VALUES ( $placeholders )) AS S ($sourceColumns)
         |ON ($onClause)
         |WHEN MATCHED THEN UPDATE SET $updateClause
         |WHEN NOT MATCHED THEN INSERT ($insertColumns)
         |VALUES($insertValues)
       """.stripMargin
    conn.prepareStatement(sql)
  }
}
