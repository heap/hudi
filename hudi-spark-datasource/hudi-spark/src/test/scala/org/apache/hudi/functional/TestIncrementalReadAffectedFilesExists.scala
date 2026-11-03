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
import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}

import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

/**
 * Counts the existence checks done on the data files of the queried window while planning and running a MOR
 * incremental query with the fallback to full table scan enabled, checking the files sequentially or in parallel.
 */
class TestIncrementalReadAffectedFilesExists extends HoodieSparkClientTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[TestIncrementalReadAffectedFilesExists])

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
    hadoopConf.unset(ExistsCountingLocalFileSystem.DELAY_MS)
    spark = null
    cleanupResources()
  }

  @Test
  def testExistsChecksOfIncrementalQueryWithFallback(): Unit = {
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

    val metaClient = createMetaClient(spark, basePath)
    val commits = metaClient.getCommitsTimeline.filterCompletedInstants().getInstants.asScala
    assertEquals(numCommits, commits.size)
    // begin instant is exclusive: the window is every commit but the first one, and its start is not archived
    val beginTs = commits.head.getTimestamp
    val windowMetadata = TimelineUtils.getCommitsMetadata(commits.tail.asJava, metaClient.getCommitsTimeline, 1)
    // the files the relation checks, in the order it checks them
    val affectedFiles = HoodieInputFormatUtils.listAffectedFilesForCommits(
      spark.sparkContext.hadoopConfiguration, metaClient.getBasePathV2, windowMetadata, "*").asScala
    val windowFiles = affectedFiles.map(_.getPath.getName)
    assertEquals(windowFiles.size, windowFiles.toSet.size)
    // the early stop below is only visible if there are more files than the pool reads ahead
    assertTrue(windowFiles.size > 40, s"expected more than 40 files in the window, got ${windowFiles.size}")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.file.impl", classOf[ExistsCountingLocalFileSystem].getName)
    hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
    // stands for the latency of a HEAD on S3
    hadoopConf.setLong(ExistsCountingLocalFileSystem.DELAY_MS, 20)

    // all the files exist: the fallback does not trigger, in both modes
    val sequential = runIncrementalQuery(beginTs, parallelism = 1)
    val parallel = runIncrementalQuery(beginTs, parallelism = 8)
    LOG.warn(s"exists, all files present, sequential: $sequential")
    LOG.warn(s"exists, all files present, parallel: $parallel")

    assertEquals(perBatchSize * (numCommits - 1), sequential.rows)
    assertEquals(sequential.rows, parallel.rows)
    // reading in parallel changes when the files are checked, not how many times
    assertEquals(sequential.exists, parallel.exists)
    assertTrue(windowFiles.forall(f => parallel.exists.getOrElse(f, 0) >= 1), "every file of the window is checked")
    assertEquals(1, sequential.maxConcurrentExists)
    assertTrue(parallel.maxConcurrentExists > 1, s"expected concurrent checks, got ${parallel.maxConcurrentExists}")

    // one file of the window is gone: the fallback triggers, in both modes
    val missingIndex = 3
    assertTrue(new java.io.File(affectedFiles(missingIndex).getPath.toUri.getPath).delete())
    val sequentialMissing = runIncrementalQuery(beginTs, parallelism = 1)
    val parallelMissing = runIncrementalQuery(beginTs, parallelism = 8)
    LOG.warn(s"exists, one file missing, sequential: $sequentialMissing")
    LOG.warn(s"exists, one file missing, parallel: $parallelMissing")

    // the full table scan still filters the records on the window: the result is the same as with all files present
    assertEquals(sequential.rows, sequentialMissing.rows)
    assertEquals(sequentialMissing.rows, parallelMissing.rows)
    // only a check finding the missing file stops the scan early, so a total below the happy path shows the fallback
    // triggered. The sequential scan makes missingIndex + 1 of them per planning,
    // the pool at most 2 * 8 more
    assertTrue(sequentialMissing.exists.getOrElse(affectedFiles(missingIndex).getPath.getName, 0) >= 1)
    assertTrue(sequentialMissing.totalExists < sequential.totalExists)
    assertTrue(parallelMissing.totalExists < parallel.totalExists)
    assertTrue(parallelMissing.totalExists >= sequentialMissing.totalExists)
    assertTrue(parallelMissing.totalExists <= sequentialMissing.totalExists / (missingIndex + 1) * (missingIndex + 1 + 2 * 8),
      s"expected at most ${2 * 8} extra checks per planning, got ${parallelMissing.totalExists} against ${sequentialMissing.totalExists}")
  }

  private def runIncrementalQuery(beginTs: String, parallelism: Int): QueryExists = {
    ExistsCountingLocalFileSystem.reset()
    val df = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, beginTs)
      .option(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key, "true")
      .option(DataSourceReadOptions.INCREMENTAL_READ_COMMIT_METADATA_PARALLELISM.key, parallelism.toString)
      .load(basePath)
    val start = System.nanoTime()
    df.queryExecution.executedPlan
    val planningMs = (System.nanoTime() - start) / 1000000
    val rows = df.count()
    QueryExists(ExistsCountingLocalFileSystem.snapshot(), ExistsCountingLocalFileSystem.maxConcurrentExists, planningMs, rows)
  }
}

/**
 * Existence checks per data file, cumulated over the query.
 */
case class QueryExists(exists: Map[String, Int], maxConcurrentExists: Int, planningMs: Long, rows: Long) {
  def totalExists: Int = exists.values.sum

  override def toString: String =
    s"$totalExists exists on ${exists.size} files (per file: ${exists.values.groupBy(identity).mapValues(_.size).toSeq.sorted.mkString(", ")}); " +
      s"max concurrent exists: $maxConcurrentExists; planning: $planningMs ms; rows: $rows"
}

/**
 * Local file system counting the existence checks of the data table's files (outside /.hoodie/), with an optional
 * delay on each check.
 */
class ExistsCountingLocalFileSystem extends LocalFileSystem {
  override def exists(f: Path): Boolean = {
    if (ExistsCountingLocalFileSystem.isDataFile(f)) {
      ExistsCountingLocalFileSystem.exists.computeIfAbsent(f.getName, _ => new AtomicInteger()).incrementAndGet()
      val inFlight = ExistsCountingLocalFileSystem.inFlight.incrementAndGet()
      ExistsCountingLocalFileSystem.maxInFlight.accumulateAndGet(inFlight, (a: Int, b: Int) => Math.max(a, b))
      try {
        val delayMs = getConf.getLong(ExistsCountingLocalFileSystem.DELAY_MS, 0)
        if (delayMs > 0) {
          Thread.sleep(delayMs)
        }
        super.exists(f)
      } finally {
        ExistsCountingLocalFileSystem.inFlight.decrementAndGet()
      }
    } else {
      super.exists(f)
    }
  }
}

object ExistsCountingLocalFileSystem {
  val DELAY_MS = "hoodie.test.exists.delay.ms"

  private val DataFile = """.+\.(parquet|log\.\d+.*)""".r
  private val exists = new ConcurrentHashMap[String, AtomicInteger]()
  private val inFlight = new AtomicInteger()
  private val maxInFlight = new AtomicInteger()

  def isDataFile(f: Path): Boolean = {
    val path = f.toUri.getPath
    !path.contains("/.hoodie/") && DataFile.pattern.matcher(f.getName).matches()
  }

  def reset(): Unit = {
    exists.clear()
    maxInFlight.set(0)
  }

  def snapshot(): Map[String, Int] = exists.asScala.map { case (k, v) => k -> v.get() }.toMap

  def maxConcurrentExists: Int = maxInFlight.get()
}
