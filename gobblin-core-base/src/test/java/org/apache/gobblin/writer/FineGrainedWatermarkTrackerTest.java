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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.source.extractor.DefaultCheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.util.ExecutorsUtils;


@Slf4j
@Test
public class FineGrainedWatermarkTrackerTest {

  /**
   * Single threaded test that creates attempts, acknowledges a few at random
   * then checks if the getCommitables method is returning the right values.
   * Runs a few iterations.
   */
  @Test
  public static void testWatermarkTracker() {
    Random random = new Random();
    Config config = ConfigFactory.empty();

    for (int j =0; j < 100; ++j) {
      FineGrainedWatermarkTracker tracker = new FineGrainedWatermarkTracker(config);

      int numWatermarks = 1 + random.nextInt(1000);
      AcknowledgableWatermark[] acknowledgableWatermarks = new AcknowledgableWatermark[numWatermarks];

      for (int i = 0; i < numWatermarks; ++i) {
        CheckpointableWatermark checkpointableWatermark = new DefaultCheckpointableWatermark("default", new LongWatermark(i));
        AcknowledgableWatermark ackable = new AcknowledgableWatermark(checkpointableWatermark);
        acknowledgableWatermarks[i] = ackable;
        tracker.track(ackable);
      }

      // Create some random holes. Don't fire acknowledgements for these messages.
      int numMissingAcks = random.nextInt(numWatermarks);
      SortedSet<Integer> holes = new TreeSet<>();
      for (int i = 0; i < numMissingAcks; ++i) {
        holes.add(random.nextInt(numWatermarks));
      }

      for (int i = 0; i < numWatermarks; ++i) {
        if (!holes.contains(i)) {
          acknowledgableWatermarks[i].ack();
        }
      }

      verifyCommitables(tracker, holes, numWatermarks-1);
      // verify that sweeping doesn't have any side effects on correctness
      tracker.sweep();
      verifyCommitables(tracker, holes, numWatermarks-1);
    }
  }

  private static void verifyCommitables(FineGrainedWatermarkTracker tracker, SortedSet<Integer> holes, long maxWatermark) {
    // commitable should be the first hole -1
    // uncommitable should be the first hole
    Map<String, CheckpointableWatermark> uncommitted = tracker.getUnacknowledgedWatermarks();
    if (holes.isEmpty()) {
      Assert.assertEquals(uncommitted.size(), 0);
    } else {
      Assert.assertEquals(uncommitted.size(), 1);
      CheckpointableWatermark uncommitable = uncommitted.get("default");
      Assert.assertEquals(((LongWatermark) uncommitable.getWatermark()).getValue(), (long) holes.first());
    }

    Map<String, CheckpointableWatermark> commitables = tracker.getCommittableWatermarks();
    if (holes.contains(0)) {
      // if the first record didn't get an ack
      Assert.assertEquals(commitables.size(), 0);
    } else {
      Assert.assertEquals(commitables.size(), 1);
      CheckpointableWatermark commitable = commitables.get("default");
      if (holes.isEmpty()) {
        Assert.assertEquals(((LongWatermark) commitable.getWatermark()).getValue(), maxWatermark);
      } else {
        Assert.assertEquals(((LongWatermark) commitable.getWatermark()).getValue(), holes.first() - 1);
      }
    }

  }

  /**
   * Tests that sweep is sweeping the correct number of entries.
   */
  @Test
  public static void testSweep() {
    Random random = new Random();

    for (int j =0; j < 1000; ++j) {
      FineGrainedWatermarkTracker tracker = new FineGrainedWatermarkTracker(ConfigFactory.empty());
      tracker.setAutoStart(false);

      int numWatermarks = 1+random.nextInt(1000);
      AcknowledgableWatermark[] acknowledgableWatermarks = new AcknowledgableWatermark[numWatermarks];

      for (int i = 0; i < numWatermarks; ++i) {
        CheckpointableWatermark checkpointableWatermark = new DefaultCheckpointableWatermark("default", new LongWatermark(i));
        AcknowledgableWatermark ackable = new AcknowledgableWatermark(checkpointableWatermark);
        acknowledgableWatermarks[i] = ackable;
        tracker.track(ackable);
      }

      int numMissingAcks = random.nextInt(numWatermarks);
      SortedSet<Integer> holes = new TreeSet<>();
      for (int i = 0; i < numMissingAcks; ++i) {
        holes.add(random.nextInt(numWatermarks));
      }

      for (int i = 0; i < numWatermarks; ++i) {
        if (!holes.contains(i)) {
          acknowledgableWatermarks[i].ack();
        }
      }

      verifyCommitables(tracker, holes, numWatermarks - 1);
      int swept = tracker.sweep();
      if (holes.isEmpty()) {
        Assert.assertEquals(swept, numWatermarks -1);
      } else {
        if (holes.contains(0)) {
          Assert.assertEquals(swept, 0);
        } else {
          Assert.assertEquals(swept, holes.first() - 1);
        }
      }
      verifyCommitables(tracker, holes, numWatermarks - 1);

    }
  }

  /**
   * A concurrent test, attempts fired in a single thread, but acks come in from multiple threads,
   * out of order.
   *
   */
  @Test
  public static void testConcurrentWatermarkTracker()
      throws IOException, InterruptedException {
    Random random = new Random();
    ScheduledExecutorService ackingService = new ScheduledThreadPoolExecutor(100, ExecutorsUtils.defaultThreadFactory());


    for (int j =0; j < 100; ++j) {
      FineGrainedWatermarkTracker tracker = new FineGrainedWatermarkTracker(ConfigFactory.empty());
      tracker.start();

      int numWatermarks = 1 + random.nextInt(1000);
      AcknowledgableWatermark[] acknowledgableWatermarks = new AcknowledgableWatermark[numWatermarks];
      SortedSet<Integer> holes = new TreeSet<>();

      final AtomicInteger numAcks = new AtomicInteger(0);

      for (int i = 0; i < numWatermarks; ++i) {
        CheckpointableWatermark checkpointableWatermark = new DefaultCheckpointableWatermark("default", new LongWatermark(i));
        final AcknowledgableWatermark ackable = new AcknowledgableWatermark(checkpointableWatermark);
        tracker.track(ackable);
        acknowledgableWatermarks[i] = ackable;
        // ack or not
        boolean ack = random.nextBoolean();
        if (ack) {
          numAcks.incrementAndGet();
          long sleepTime = random.nextInt(100);
          ackingService.schedule(new Callable<Object>() {
            @Override
            public Object call()
                throws Exception {
              ackable.ack();
              numAcks.decrementAndGet();
              return null;
            }
          }, sleepTime, TimeUnit.MILLISECONDS);
        } else {
          holes.add(i);
        }
      }


      while (numAcks.get() != 0) {
        log.info("Waiting for " + numAcks.get() + " acks");
        Thread.sleep(100);
      }

      verifyCommitables(tracker, holes, numWatermarks-1);
      tracker.close();
    }
  }



}
