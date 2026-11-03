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
import java.util.Collections;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  @Test
  public void testAnyMatchInOrderMatchesSequentialScan() throws IOException {
    List<Integer> items = IntStream.range(0, 40).boxed().collect(Collectors.toList());
    for (int parallelism : new int[] {1, 2, 8, 64}) {
      assertFalse(TimelineUtils.anyMatchInOrder(items, i -> false, parallelism), "no item matches, parallelism " + parallelism);
      assertFalse(TimelineUtils.anyMatchInOrder(Collections.<Integer>emptyList(), i -> true, parallelism));
      for (int matching : new int[] {0, 1, 17, 39}) {
        assertTrue(TimelineUtils.anyMatchInOrder(items, i -> i == matching, parallelism),
            "item " + matching + " matches, parallelism " + parallelism);
      }
      assertTrue(TimelineUtils.anyMatchInOrder(items, i -> i % 10 == 9, parallelism));
    }
  }

  @Test
  public void testAnyMatchInOrderEvaluatesEveryItemOnceWhenNoneMatches() throws IOException {
    List<Integer> items = IntStream.range(0, 40).boxed().collect(Collectors.toList());
    for (int parallelism : new int[] {1, 8}) {
      Map<Integer, AtomicInteger> calls = new ConcurrentHashMap<>();
      assertFalse(TimelineUtils.anyMatchInOrder(items, i -> {
        calls.computeIfAbsent(i, k -> new AtomicInteger()).incrementAndGet();
        return false;
      }, parallelism));
      assertEquals(40, calls.size());
      assertTrue(calls.values().stream().allMatch(c -> c.get() == 1), "each item is evaluated once, parallelism " + parallelism);
    }
  }

  @Test
  public void testAnyMatchInOrderStopsAfterTheFirstMatch() throws IOException {
    List<Integer> items = IntStream.range(0, 200).boxed().collect(Collectors.toList());
    AtomicInteger sequentialCalls = new AtomicInteger();
    assertTrue(TimelineUtils.anyMatchInOrder(items, i -> {
      sequentialCalls.incrementAndGet();
      return i == 3;
    }, 1));
    assertEquals(4, sequentialCalls.get());

    AtomicInteger parallelCalls = new AtomicInteger();
    assertTrue(TimelineUtils.anyMatchInOrder(items, i -> {
      parallelCalls.incrementAndGet();
      sleep(5);
      return i == 3;
    }, 8));
    // at most twice as many items as threads are ahead of the one being decided
    assertTrue(parallelCalls.get() <= 4 + 2 * 8, "expected at most 20 evaluations, got " + parallelCalls.get());
  }

  @Test
  public void testAnyMatchInOrderDecidesInOrder() {
    List<Integer> items = IntStream.range(0, 40).boxed().collect(Collectors.toList());
    for (int parallelism : new int[] {1, 8}) {
      // item 3 fails late and item 10 matches at once: a sequential scan hits the failure first
      IOException e = assertThrows(IOException.class, () -> TimelineUtils.anyMatchInOrder(items, i -> {
        if (i == 3) {
          sleep(100);
          throw new IOException("failed 3");
        }
        return i == 10;
      }, parallelism));
      assertEquals("failed 3", e.getMessage());
    }
  }

  @Test
  public void testAnyMatchInOrderReturnsTrueBeforeALaterFailure() throws IOException {
    List<Integer> items = IntStream.range(0, 40).boxed().collect(Collectors.toList());
    for (int parallelism : new int[] {1, 8}) {
      // item 3 matches and item 5 fails at once: a sequential scan stops at 3 and never reaches the failure
      assertTrue(TimelineUtils.anyMatchInOrder(items, i -> {
        if (i == 5) {
          throw new IOException("failed 5");
        }
        return i == 3;
      }, parallelism));
    }
  }

  @Test
  public void testAnyMatchInOrderRethrowsFailuresAsIs() {
    List<Integer> items = IntStream.range(0, 40).boxed().collect(Collectors.toList());
    for (int parallelism : new int[] {1, 8}) {
      IOException io = assertThrows(IOException.class, () -> TimelineUtils.anyMatchInOrder(items, i -> {
        throw new IOException("io " + i);
      }, parallelism));
      assertEquals("io 0", io.getMessage());
      HoodieException runtime = assertThrows(HoodieException.class, () -> TimelineUtils.anyMatchInOrder(items, i -> {
        throw new HoodieException("runtime " + i);
      }, parallelism));
      assertEquals("runtime 0", runtime.getMessage());
    }
  }

  @Test
  public void testAnyMatchInOrderUsesSeveralThreads() throws IOException {
    Set<String> threads = ConcurrentHashMap.newKeySet();
    TimelineUtils.anyMatchInOrder(IntStream.range(0, 40).boxed().collect(Collectors.toList()), i -> {
      threads.add(Thread.currentThread().getName());
      sleep(5);
      return false;
    }, 8);
    assertTrue(threads.size() > 1, "expected the items to be evaluated by several threads");
    assertFalse(threads.contains(Thread.currentThread().getName()));
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
