/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the parallel reads of instant metadata in {@link TimelineUtils}.
 */
public class TestTimelineUtilsParallelReads {

  private static List<HoodieInstant> deltaCommits(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, String.format("%03d", i)))
        .collect(Collectors.toList());
  }

  @Test
  public void testResultsFollowInstantOrder() {
    List<HoodieInstant> instants = deltaCommits(24);
    Set<String> threads = ConcurrentHashMap.newKeySet();
    // earlier instants take longer, so that they complete last
    List<String> results = TimelineUtils.mapInstantsInOrder(instants, instant -> {
      threads.add(Thread.currentThread().getName());
      sleep(48 - 2 * Integer.parseInt(instant.getTimestamp()));
      return instant.getTimestamp();
    }, 8);

    assertEquals(instants.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()), results);
    assertTrue(threads.size() > 1, "expected the instants to be read by several threads");
  }

  @Test
  public void testSequentialReadRunsOnCallerThread() {
    Set<String> threads = ConcurrentHashMap.newKeySet();
    TimelineUtils.mapInstantsInOrder(deltaCommits(5), instant -> threads.add(Thread.currentThread().getName()), 1);
    assertEquals(1, threads.size());
    assertTrue(threads.contains(Thread.currentThread().getName()));
  }

  @Test
  public void testEarliestFailureIsRethrown() {
    // instant 010 fails right away, instant 003 fails later: the failure of 003 is the one a sequential read would hit
    HoodieException e = assertThrows(HoodieException.class, () -> TimelineUtils.mapInstantsInOrder(deltaCommits(16), instant -> {
      if (instant.getTimestamp().equals("003")) {
        sleep(100);
        throw new HoodieException("failed " + instant.getTimestamp());
      }
      if (instant.getTimestamp().equals("010")) {
        throw new HoodieException("failed " + instant.getTimestamp());
      }
      return instant;
    }, 8));
    assertEquals("failed 003", e.getMessage());
  }

  @Test
  public void testErrorIsRethrownAsIs() {
    OutOfMemoryError e = assertThrows(OutOfMemoryError.class, () -> TimelineUtils.mapInstantsInOrder(deltaCommits(4), instant -> {
      throw new OutOfMemoryError("oom " + instant.getTimestamp());
    }, 2));
    assertEquals("oom 000", e.getMessage());
  }

  @Test
  public void testPendingResultsAreBounded() {
    AtomicInteger read = new AtomicInteger();
    AtomicInteger consumed = new AtomicInteger();
    AtomicInteger maxPending = new AtomicInteger();
    // the consumer is slower than the readers, which must wait instead of reading every instant ahead
    Consumer<HoodieInstant> slowConsumer = instant -> {
      sleep(5);
      consumed.incrementAndGet();
    };
    TimelineUtils.forEachInstantInOrder(deltaCommits(40), instant -> {
      maxPending.accumulateAndGet(read.incrementAndGet() - consumed.get(), Math::max);
      return instant;
    }, 4, slowConsumer);
    assertEquals(40, consumed.get());
    assertTrue(maxPending.get() <= 8, "expected at most 8 pending results, got " + maxPending.get());
  }

  @Test
  public void testParallelReadsMatchSequentialReads() throws IOException {
    List<HoodieInstant> instants = deltaCommits(40);
    Map<HoodieInstant, byte[]> details = new HashMap<>();
    for (HoodieInstant instant : instants) {
      int i = Integer.parseInt(instant.getTimestamp());
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      for (int p = 0; p <= i % 3; p++) {
        String partition = "env_id=" + (i % 7) + "/week=" + p;
        HoodieWriteStat stat = new HoodieWriteStat();
        stat.setPartitionPath(partition);
        stat.setFileId("file-" + i + "-" + p);
        stat.setPath(partition + "/.file-" + i + "-" + p + "_" + instant.getTimestamp() + ".log.1_0-1-1");
        stat.setFileSizeInBytes(1000L + i);
        metadata.addWriteStat(partition, stat);
      }
      details.put(instant, getUTF8Bytes(metadata.toJsonString()));
    }
    HoodieTimeline timeline = new HoodieDefaultTimeline(instants.stream(), instant -> Option.of(details.get(instant)));

    List<String> sequential = TimelineUtils.getCommitsMetadata(instants, timeline, 1).stream()
        .map(TestTimelineUtilsParallelReads::toJson).collect(Collectors.toList());
    List<String> parallel = TimelineUtils.getCommitsMetadata(instants, timeline, 8).stream()
        .map(TestTimelineUtilsParallelReads::toJson).collect(Collectors.toList());
    assertEquals(sequential, parallel);

    assertEquals(TimelineUtils.getAffectedPartitions(timeline), TimelineUtils.getAffectedPartitions(timeline, 8));
    assertEquals(TimelineUtils.getWrittenPartitions(timeline), TimelineUtils.getWrittenPartitions(timeline, 8));
  }

  private static String toJson(HoodieCommitMetadata metadata) {
    try {
      return metadata.toJsonString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(Math.max(0, millis));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
