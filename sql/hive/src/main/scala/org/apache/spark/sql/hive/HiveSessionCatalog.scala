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

package org.apache.spark.sql.hive

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType


class HiveSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    client: HiveClient,
    context: HiveContext,
    functionRegistry: FunctionRegistry,
    conf: SQLConf)
  extends SessionCatalog(externalCatalog, functionRegistry, conf) {

  override def setCurrentDatabase(db: String): Unit = {
    super.setCurrentDatabase(db)
    client.setCurrentDatabase(db)
  }

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    val table = formatTableName(name.table)
    if (name.database.isDefined || !tempTables.contains(table)) {
      val newName = name.copy(table = table)
      metastoreCatalog.lookupRelation(newName, alias)
    } else {
      val relation = tempTables(table)
      val tableWithQualifiers = SubqueryAlias(table, relation)
      // If an alias was specified by the lookup, wrap the plan in a subquery so that
      // attributes are properly qualified with this alias.
      alias.map(a => SubqueryAlias(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
    }
  }

  // ----------------------------------------------------------------
  // | Methods and fields for interacting with HiveMetastoreCatalog |
  // ----------------------------------------------------------------

  override def getDefaultDBPath(db: String): String = {
    val defaultPath = context.hiveconf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE)
    new Path(new Path(defaultPath), db + ".db").toString
  }

  // Catalog for handling data source tables. TODO: This really doesn't belong here since it is
  // essentially a cache for metastore tables. However, it relies on a lot of session-specific
  // things so it would be a lot of work to split its functionality between HiveSessionCatalog
  // and HiveCatalog. We should still do it at some point...
  private val metastoreCatalog = new HiveMetastoreCatalog(client, context)

  val ParquetConversions: Rule[LogicalPlan] = metastoreCatalog.ParquetConversions
  val OrcConversions: Rule[LogicalPlan] = metastoreCatalog.OrcConversions
  val CreateTables: Rule[LogicalPlan] = metastoreCatalog.CreateTables
  val PreInsertionCasts: Rule[LogicalPlan] = metastoreCatalog.PreInsertionCasts

  override def refreshTable(name: TableIdentifier): Unit = {
    metastoreCatalog.refreshTable(name)
  }

  def invalidateTable(name: TableIdentifier): Unit = {
    metastoreCatalog.invalidateTable(name)
  }

  def invalidateCache(): Unit = {
    metastoreCatalog.cachedDataSourceTables.invalidateAll()
  }

  def createDataSourceTable(
                             name: TableIdentifier,
                             userSpecifiedSchema: Option[StructType],
                             partitionColumns: Array[String],
                             bucketSpec: Option[BucketSpec],
                             provider: String,
                             options: Map[String, String],
                             isExternal: Boolean): Unit = {
    metastoreCatalog.createDataSourceTable(
      name, userSpecifiedSchema, partitionColumns, bucketSpec, provider, options, isExternal)
  }

  def hiveDefaultTableFilePath(name: TableIdentifier): String = {
    metastoreCatalog.hiveDefaultTableFilePath(name)
  }

  // For testing only
  private[hive] def getCachedDataSourceTable(table: TableIdentifier): LogicalPlan = {
    val key = metastoreCatalog.getQualifiedTableName(table)
    metastoreCatalog.cachedDataSourceTables.getIfPresent(key)
  }

  /**
   * Generate Create table DDL string for the specified tableIdentifier
   * that is from Hive metastore
   */
  override def generateTableDDL(name: TableIdentifier): String = {
    val ct = this.getTable(name)
    val sb = new StringBuilder("CREATE ")
    val processedProperties = scala.collection.mutable.ArrayBuffer.empty[String]

    if (ct.tableType == CatalogTableType.VIRTUAL_VIEW) {
      sb.append(" VIEW "+ct.qualifiedName+" AS " + ct.viewOriginalText.getOrElse(""))
    } else {
      // TEMPORARY keyword is not applicable for HIVE DDL from Spark SQL yet.
      if (ct.tableType == CatalogTableType.EXTERNAL_TABLE) {
        processedProperties += "EXTERNAL"
        sb.append(" EXTERNAL TABLE " + ct.qualifiedName)
      } else {
        sb.append(" TABLE " + ct.qualifiedName)
      }
      // column list
      val cols = ct.schema map { col =>
        col.name + " " + col.dataType + (col.comment.getOrElse("") match {
          case cmt: String if cmt.length > 0 => " COMMENT '" + escapeHiveCommand(cmt) + "'"
          case _ => ""
        })
        // hive ddl does not honor NOT NULL, it is always default to be nullable
      }
      sb.append(cols.mkString("(", ", ", ")")+"\n")

      // table comment
      sb.append(" " +
        ct.properties.getOrElse("comment", new String) match {
        case tcmt: String if tcmt.trim.length > 0 =>
          processedProperties += "comment"
          " COMMENT '" + escapeHiveCommand(tcmt.trim) + "'\n"
        case _ => ""
      })

      // partitions
      val partCols = ct.partitionColumns map { col =>
        col.name + " " + col.dataType + (col.comment.getOrElse("") match {
          case cmt: String if cmt.length > 0 => " COMMENT '" + escapeHiveCommand(cmt) + "'"
          case _ => ""
        })
      }
      if (partCols != null && partCols.size > 0) {
        sb.append(" PARTITIONED BY ")
        sb.append(partCols.mkString("( ", ", ", " )")+"\n")
      }

      // sort bucket
      if (ct.bucketColumns.size > 0) {
        processedProperties += "SORTBUCKETCOLSPREFIX"
        sb.append(" CLUSTERED BY ")
        sb.append(ct.bucketColumns.mkString("( ", ", ", " )"))

        // TODO sort columns don't have the the right scala types yet. need to adapt to Hive Order
        if (ct.sortColumns.size > 0) {
          sb.append(" SORTED BY ")
          sb.append(ct.sortColumns.map(_.name).mkString("( ", ", ", " )"))
        }
        sb.append(" INTO " + ct.numBuckets + " BUCKETS\n")
      }

      // TODO CatalogTable does not implement skew spec yet
      // skew spec
      // TODO StorageHandler case is not handled yet, since CatalogTable does not have it yet
      // row format
      sb.append(" ROW FORMAT ")

      val serdeProps = ct.storage.serdeProperties
      val delimiterPrefixes =
        Seq("FIELDS TERMINATED BY",
        "COLLECTION ITEMS TERMINATED BY",
        "MAP KEYS TERMINATED BY",
        "LINES TERMINATED BY",
        "NULL DEFINED AS")

      val delimiters = Seq(
        serdeProps.get("field.delim"),
        serdeProps.get("colelction.delim"),
        serdeProps.get("mapkey.delim"),
        serdeProps.get("line.delim"),
        serdeProps.get("serialization.null.format")).zipWithIndex

      val delimiterStrs = delimiters collect {
        case (Some(ch), i) =>
          delimiterPrefixes(i) + " '" +
            escapeHiveCommand(ch) +
            "' "
      }
      if (delimiterStrs.size > 0){
        sb.append("DELIMITED ")
        sb.append(delimiterStrs.mkString(" ")+"\n")
      }else{
        sb.append("SERDE '")
        sb.append(escapeHiveCommand(ct.storage.serde.getOrElse(""))+ "' \n")
      }

      sb.append("STORED AS INPUTFORMAT '" +
        escapeHiveCommand(ct.storage.inputFormat.getOrElse("")) + "' \n")
      sb.append("OUTPUTFORMAT  '" +
        escapeHiveCommand(ct.storage.outputFormat.getOrElse(""))+"' \n")

      // table location
      sb.append("LOCATION '" +
        escapeHiveCommand(ct.storage.locationUri.getOrElse(""))+"' \n")

      // table properties
      val propertPairs = ct.properties collect {
        case (k, v) if !processedProperties.contains(k) =>
          "'" + escapeHiveCommand(k) + "'='"+escapeHiveCommand(v)+"'"
      }
      if(propertPairs.size>0)
        sb.append("TBLPROPERTIES " + propertPairs.mkString("( ", ", \n", " )")+"\n")

    }
    sb.toString()
  }

  private def escapeHiveCommand(str: String): String = {
    str.map{c =>
      if (c == '\'' || c == ';'){
        '\\'
      } else {
        c
      }
    }
  }
}
