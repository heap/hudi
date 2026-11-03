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

package org.apache.hudi.functional

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}

import org.apache.hadoop.fs.{FSDataInputStream, LocalFileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

/**
 * Counts how many times each commit file of the queried window is opened while planning and running a
 * MOR incremental query, reading the commit metadata sequentially or in parallel.
 */
class TestIncrementalReadCommitMetadataOpens extends HoodieSparkClientTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[TestIncrementalReadCommitMetadataOpens])

  var spark: SparkSession = _
  private val numCommits = 20
  private val perBatchSize = 10

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown() = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.unset("fs.file.impl")
    hadoopConf.unset("fs.file.impl.disable.cache")
    hadoopConf.unset(OpenCountingLocalFileSystem.DELAY_MS)
    spark = null
    cleanupResources()
  }

  @Test
  def testCommitFileOpensDuringIncrementalQuery(): Unit = {
    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
      DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.MERGE_ON_READ.name(),
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieMetadataConfig.ENABLE.key -> "false",
      // no embedded timeline server: the test only needs local files
      HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key -> "false",
      HoodieWriteConfig.MARKERS_TYPE.key -> "DIRECT",
      // keep every commit in the active timeline
      "hoodie.cleaner.commits.retained" -> "35",
      "hoodie.keep.min.commits" -> "40",
      "hoodie.keep.max.commits" -> "50"
    )
    for (i <- 1 to numCommits) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), perBatchSize)).asScala.toList
      spark.read.json(spark.sparkContext.parallelize(records, 1))
        .write.format("org.apache.hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)
    }

    val commits = createMetaClient(spark, basePath).getCommitsTimeline.filterCompletedInstants().getInstants.asScala
    assertEquals(numCommits, commits.size)
    // begin instant is exclusive: the window is every commit but the first one, and its start is not archived
    val beginTs = commits.head.getTimestamp
    val window = commits.tail.map(_.getFileName).toSet

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.file.impl", classOf[OpenCountingLocalFileSystem].getName)
    hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
    // stands for the latency of a GET on S3
    hadoopConf.setLong(OpenCountingLocalFileSystem.DELAY_MS, 20)

    val sequential = runIncrementalQuery(beginTs, parallelism = 1)
    val parallel = runIncrementalQuery(beginTs, parallelism = 8)
    LOG.warn(s"commit file opens, sequential: $sequential")
    LOG.warn(s"commit file opens, parallel: $parallel")

    assertEquals(perBatchSize * window.size, sequential.rows)
    assertEquals(sequential.rows, parallel.rows)
    // reading in parallel changes when commit files are opened, not how many times
    assertEquals(sequential.afterLoad, parallel.afterLoad)
    assertEquals(sequential.afterPlanning, parallel.afterPlanning)
    assertEquals(sequential.afterCount, parallel.afterCount)
    assertTrue(window.forall(f => parallel.afterCount.getOrElse(f, 0) >= 1), "every commit of the window is read")
    assertEquals(1, sequential.maxConcurrentOpens)
    assertTrue(parallel.maxConcurrentOpens > 1, s"expected concurrent reads, got ${parallel.maxConcurrentOpens}")
  }

  private def runIncrementalQuery(beginTs: String, parallelism: Int): QueryOpens = {
    OpenCountingLocalFileSystem.reset()
    val df = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, beginTs)
      .option(DataSourceReadOptions.INCREMENTAL_READ_COMMIT_METADATA_PARALLELISM.key, parallelism.toString)
      .load(basePath)
    val afterLoad = OpenCountingLocalFileSystem.snapshot()
    val start = System.nanoTime()
    df.queryExecution.executedPlan
    val planningMs = (System.nanoTime() - start) / 1000000
    val afterPlanning = OpenCountingLocalFileSystem.snapshot()
    val rows = df.count()
    val afterCount = OpenCountingLocalFileSystem.snapshot()
    QueryOpens(afterLoad, afterPlanning, afterCount, OpenCountingLocalFileSystem.maxConcurrentOpens, planningMs, rows)
  }
}

/**
 * Opens per commit file, cumulated at each stage of the query.
 */
case class QueryOpens(afterLoad: Map[String, Int],
                      afterPlanning: Map[String, Int],
                      afterCount: Map[String, Int],
                      maxConcurrentOpens: Int,
                      planningMs: Long,
                      rows: Long) {
  override def toString: String = {
    def summary(opens: Map[String, Int]): String =
      s"${opens.values.sum} opens of ${opens.size} commit files (per file: ${opens.values.groupBy(identity).mapValues(_.size).toSeq.sorted.mkString(", ")})"
    s"load: ${summary(afterLoad)}; planning: ${summary(afterPlanning)}; count: ${summary(afterCount)}; " +
      s"max concurrent opens: $maxConcurrentOpens; planning: $planningMs ms; rows: $rows"
  }
}

/**
 * Local file system counting the opens of the data table's commit files (under /.hoodie/, metadata table excluded),
 * with an optional delay on each open.
 */
class OpenCountingLocalFileSystem extends LocalFileSystem {
  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    if (OpenCountingLocalFileSystem.isCommitFile(f)) {
      OpenCountingLocalFileSystem.opens.computeIfAbsent(f.getName, _ => new AtomicInteger()).incrementAndGet()
      val inFlight = OpenCountingLocalFileSystem.inFlight.incrementAndGet()
      OpenCountingLocalFileSystem.maxInFlight.accumulateAndGet(inFlight, (a: Int, b: Int) => Math.max(a, b))
      try {
        val delayMs = getConf.getLong(OpenCountingLocalFileSystem.DELAY_MS, 0)
        if (delayMs > 0) {
          Thread.sleep(delayMs)
        }
        super.open(f, bufferSize)
      } finally {
        OpenCountingLocalFileSystem.inFlight.decrementAndGet()
      }
    } else {
      super.open(f, bufferSize)
    }
  }
}

object OpenCountingLocalFileSystem {
  val DELAY_MS = "hoodie.test.commit.open.delay.ms"

  private val CommitFile = """\d+\.(commit|deltacommit|replacecommit)""".r
  private val opens = new ConcurrentHashMap[String, AtomicInteger]()
  private val inFlight = new AtomicInteger()
  private val maxInFlight = new AtomicInteger()

  def isCommitFile(f: Path): Boolean = {
    val path = f.toUri.getPath
    path.contains("/.hoodie/") && !path.contains("/.hoodie/metadata/") && CommitFile.pattern.matcher(f.getName).matches()
  }

  def reset(): Unit = {
    opens.clear()
    maxInFlight.set(0)
  }

  def snapshot(): Map[String, Int] = opens.asScala.map { case (k, v) => k -> v.get() }.toMap

  def maxConcurrentOpens: Int = maxInFlight.get()
}
