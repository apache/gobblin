/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import javax.annotation.concurrent.NotThreadSafe;
import lombok.extern.slf4j.Slf4j;

import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.util.ConfigUtils;
import gobblin.util.ExecutorsUtils;


/**
 * A class to handle fine-grain watermarks.
 * Thread-safe only if you know what you are doing :)
 * TODO: impact of watermarks that go unacknowledged for a very long time
 * TODO: performance benchmarks
 * TODO: thread-safety docs
 * TODO: Instrumentation
 *
 */
@NotThreadSafe
@Slf4j
public class FineGrainedWatermarkTracker implements Closeable {

  public static final String WATERMARK_TRACKER_SWEEP_INTERVAL_MS = "watermark.tracker.sweepIntervalMillis";
  public static final Long WATERMARK_TRACKER_SWEEP_INTERVAL_MS_DEFAULT = 100L;

  private static final long MILLIS_TO_NANOS = 1000 * 1000;
  private final Map<String, Deque<AcknowledgableWatermark>> _watermarksMap;
  private final long _sweepIntervalMillis;
  private ScheduledExecutorService _executorService;


  public FineGrainedWatermarkTracker(Config config) {
    _watermarksMap = new HashMap<>();
    _sweepIntervalMillis = ConfigUtils.getLong(config, WATERMARK_TRACKER_SWEEP_INTERVAL_MS, WATERMARK_TRACKER_SWEEP_INTERVAL_MS_DEFAULT);
  }

  /**
   * Record an attempt for a watermark.
   * Assumptions: Attempt is called sequentially from the same thread for watermarks that are
   * progressively increasing.
   */
  public void attempt(AcknowledgableWatermark acknowledgableWatermark) {

    String source = acknowledgableWatermark.getCheckpointableWatermark().getSource();
    Deque<AcknowledgableWatermark> sourceWatermarks = _watermarksMap.get(source);
    if (sourceWatermarks == null) {
      sourceWatermarks = new ConcurrentLinkedDeque<>();
      _watermarksMap.put(source, sourceWatermarks);
    }
    sourceWatermarks.add(acknowledgableWatermark);
  }


  public Map<String, CheckpointableWatermark> getCommittableWatermarks() {
    Map<String, CheckpointableWatermark> commitableWatermarks = new HashMap<String, CheckpointableWatermark>(_watermarksMap.size());
    for (Map.Entry<String, Deque<AcknowledgableWatermark>> entry: _watermarksMap.entrySet()) {
      String source = entry.getKey();
      Iterable<AcknowledgableWatermark> watermarks = entry.getValue();
      AcknowledgableWatermark highestWatermark = null;
      for (AcknowledgableWatermark watermark: watermarks) {
        if (watermark.isAcked()) {
          highestWatermark = watermark;
        } else {
          // hopefully we've already found the highest contiguous acked watermark
          break;
        }
      }
      if (highestWatermark != null) {
        commitableWatermarks.put(source, highestWatermark.getCheckpointableWatermark());
      }
    }
    return commitableWatermarks;
  }

  public Map<String, CheckpointableWatermark> getUnacknowledgedWatermarks() {
    Map<String, CheckpointableWatermark> unackedWatermarks = new HashMap<>(_watermarksMap.size());
    for (Map.Entry<String, Deque<AcknowledgableWatermark>> entry: _watermarksMap.entrySet()) {
      String source = entry.getKey();
      Iterable<AcknowledgableWatermark> watermarks = entry.getValue();
      AcknowledgableWatermark lowestUnacked = null;
      for (AcknowledgableWatermark watermark: watermarks) {
        if (!watermark.isAcked()) {
          lowestUnacked = watermark;
          break;
        }
      }
      if (lowestUnacked != null) {
        unackedWatermarks.put(source, lowestUnacked.getCheckpointableWatermark());
      }
    }
    return unackedWatermarks;
  }

  /**
   * Start the sweeper thread
   */
  public void start() {
    _executorService = new ScheduledThreadPoolExecutor(1, ExecutorsUtils.newThreadFactory(
        Optional.of(LoggerFactory.getLogger(FineGrainedWatermarkTracker.class))));
    _executorService.scheduleAtFixedRate(_sweeper, 0, _sweepIntervalMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close()
      throws IOException {
    if (_executorService != null) {
      _executorService.shutdown();
    }
  }

  /**
   * A helper method to garbage collect acknowledged watermarks
   * @return number of elements collected
   */
  @VisibleForTesting
  int sweep() {
    long startTime = System.nanoTime();
    int swept = 0;
    for (Map.Entry<String, Deque<AcknowledgableWatermark>> entry : _watermarksMap.entrySet()) {
      Deque<AcknowledgableWatermark> watermarks = entry.getValue();
      /**
       * Keep popping acked elements from the front as long as their next element is also acked.
       * So: Acked_A -> Acked_B -> Not-Acked_C -> ...  becomes
       *     Acked_B -> Not-Acked_C -> ...
       *
       * We keep the acked element around because that represents the highest contiguous acked watermark.
       */
      boolean continueIteration = true;
      while (continueIteration) {
        Iterator<AcknowledgableWatermark> iter = watermarks.iterator();
        if (!iter.hasNext()) {  // null
          continueIteration = false;
          continue;
        }

        AcknowledgableWatermark first = iter.next();
        if (first.isAcked()) {
          if (!iter.hasNext()) { // Acked_A -> null
            continueIteration = false;
            continue;
          }
          AcknowledgableWatermark second = iter.next();
          if ((second != null) && second.isAcked()) { // Acked_A -> Acked_B -> ...
            watermarks.pop();
            swept++;
          } else { // Acked_A -> Not_Acked_B
            continueIteration = false;
          }
        } else { // Not_Acked_A -> ..
          continueIteration = false;
        }
      }
    }
    long duration = (System.nanoTime() - startTime)/ MILLIS_TO_NANOS;
    log.debug("Swept {} watermarks in {} millis", swept, duration);
    return swept;
  }




  private final Runnable _sweeper = new Runnable() {
    @Override
    public void run() {
      sweep();
    }
  };
}
