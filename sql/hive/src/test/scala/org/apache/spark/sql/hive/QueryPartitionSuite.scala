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

import com.google.common.io.Files

import org.apache.spark.util.Utils
import org.apache.spark.sql.{QueryTest, _}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class QueryPartitionSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext.implicits._

  test("SPARK-5068: query data when path doesn't exist") {
    withSQLConf((SQLConf.HIVE_VERIFY_PARTITION_PATH.key, "true")) {
      val testData = sparkContext.parallelize(
        (1 to 10).map(i => TestData(i, i.toString))).toDF()
      testData.registerTempTable("testData")

      val tmpDir = Files.createTempDir()
      // create the table for test
      sql(s"CREATE TABLE table_with_partition(key int,value string) " +
        s"PARTITIONED by (ds string) location '${tmpDir.toURI.toString}' ")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='1') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='2') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='3') " +
        "SELECT key,value FROM testData")
      sql("INSERT OVERWRITE TABLE table_with_partition  partition (ds='4') " +
        "SELECT key,value FROM testData")

      // test for the exist path
      checkAnswer(sql("select key,value from table_with_partition"),
        testData.toDF.collect ++ testData.toDF.collect
          ++ testData.toDF.collect ++ testData.toDF.collect)

      // delete the path of one partition
      tmpDir.listFiles
        .find { f => f.isDirectory && f.getName().startsWith("ds=") }
        .foreach { f => Utils.deleteRecursively(f) }

      // test for after delete the path
      checkAnswer(sql("select key,value from table_with_partition"),
        testData.toDF.collect ++ testData.toDF.collect ++ testData.toDF.collect)

      sql("DROP TABLE table_with_partition")
      sql("DROP TABLE createAndInsertTest")
    }
  }

  test("SPARK-10673"){
    withSQLConf((SQLConf.HIVE_VERIFY_PARTITION_PATH.key, "true")) {
      val toReset = hiveContext.getConf("hive.exec.dynamic.partition.mode")
      hiveContext.setConf("hive.exec.dynamic.partition.mode", "nostrict")
      try {
        val tmpDir = Files.createTempDir()
        sql( s"""create table SPARK_10673 (data int)
        partitioned by (year int, month int)
        row format delimited fields terminated by \",\"
        LOCATION '${tmpDir.toURI.toString}'""".stripMargin)

        val df1 = sql("select 1, 2015, 1")
        df1.write.partitionBy("year", "month").mode(SaveMode.Append).saveAsTable("SPARK_10673")
        val df2 = sql("select 2, 2015, 2")
        df2.write.partitionBy("year", "month").mode(SaveMode.Append).saveAsTable("SPARK_10673")
        val df3 = sql("select 3, 2014, 1")
        df3.write.partitionBy("year", "month").mode(SaveMode.Append).saveAsTable("SPARK_10673")
        val df4 = sql("select 4, 2014, 2")
        df4.write.partitionBy("year", "month").mode(SaveMode.Append).saveAsTable("SPARK_10673")
        val df5 = sql("select 5, 2013, 1")
        df5.write.partitionBy("year", "month").mode(SaveMode.Append).saveAsTable("SPARK_10673")

        assert(sql("select * from SPARK_10673  where year=2015").collect().size === 2)
        assert(sql("select * from SPARK_10673  where month=1").collect().size === 3)
        assert(sql("select * from SPARK_10673  where year=2015 and month=2").collect().size === 1)

        sql("DROP TABLE SPARK_10673")
      } finally{
          hiveContext.setConf("hive.exec.dynamic.partition.mode", toReset)
      }
    }
  }
}
