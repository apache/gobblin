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

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.MetricNames;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.source.extractor.CheckpointableWatermark;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * A class to handle fine-grain watermarks.
 * Thread-safe only if you know what you are doing :)
 *
 */
@NotThreadSafe
@Slf4j
public class FineGrainedWatermarkTracker implements Instrumentable, Closeable {

  public static final String WATERMARK_TRACKER_SWEEP_INTERVAL_MS = "watermark.tracker.sweepIntervalMillis";
  public static final Long WATERMARK_TRACKER_SWEEP_INTERVAL_MS_DEFAULT = 100L; // 100 milliseconds
  private static final String WATERMARK_TRACKER_STABILITY_CHECK_INTERVAL_MS = "watermark.tracker.stabilityCheckIntervalMillis";
  private static final Long WATERMARK_TRACKER_STABILITY_CHECK_INTERVAL_MS_DEFAULT = 10000L; // 10 seconds
  private static final String WATERMARK_TRACKER_LAG_THRESHOLD = "watermark.tracker.lagThreshold";
  private static final Long WATERMARK_TRACKER_LAG_THRESHOLD_DEFAULT = 100000L; // 100,000 unacked watermarks

  private static final String WATERMARKS_INSERTED_METER = "watermark.tracker.inserted";
  private static final String WATERMARKS_SWEPT_METER = "watermark.tracker.swept";



  private static final long MILLIS_TO_NANOS = 1000 * 1000;
  private final Map<String, Deque<AcknowledgableWatermark>> _watermarksMap;
  private final long _sweepIntervalMillis;
  private final long _stabilityCheckIntervalMillis;
  private final long _watermarkLagThreshold;
  private ScheduledExecutorService _executorService;


  private final boolean _instrumentationEnabled;

  private MetricContext _metricContext;
  protected final Closer _closer;
  private Meter _watermarksInserted;
  private Meter _watermarksSwept;

  private final AtomicBoolean _started;
  private final AtomicBoolean _abort;

  private boolean _autoStart = true;



  public FineGrainedWatermarkTracker(Config config) {
    _watermarksMap = new HashMap<>();
    _sweepIntervalMillis = ConfigUtils.getLong(config, WATERMARK_TRACKER_SWEEP_INTERVAL_MS,
        WATERMARK_TRACKER_SWEEP_INTERVAL_MS_DEFAULT);
    _stabilityCheckIntervalMillis = ConfigUtils.getLong(config, WATERMARK_TRACKER_STABILITY_CHECK_INTERVAL_MS,
        WATERMARK_TRACKER_STABILITY_CHECK_INTERVAL_MS_DEFAULT);
    _watermarkLagThreshold = ConfigUtils.getLong(config, WATERMARK_TRACKER_LAG_THRESHOLD,
        WATERMARK_TRACKER_LAG_THRESHOLD_DEFAULT);

    _instrumentationEnabled = GobblinMetrics.isEnabled(config);
    _closer = Closer.create();
    _metricContext = _closer.register(Instrumented.getMetricContext(ConfigUtils.configToState(config),
        this.getClass()));

    regenerateMetrics();
    _started = new AtomicBoolean(false);
    _abort = new AtomicBoolean(false);

    _sweeper = new Runnable() {
      @Override
      public void run() {
        sweep();
      }
    };


    _stabilityChecker = new Runnable() {
      @Override
      public void run() {
        checkStability();
      }
    };

  }


  @VisibleForTesting
  /**
   * Set the tracker's auto start behavior. Used for testing only.
   */
  void setAutoStart(boolean autoStart) {
    _autoStart = autoStart;
  }

  /**
   * Track a watermark.
   * Assumptions: Track is called sequentially from the same thread for watermarks that are
   * progressively increasing.
   */
  public void track(AcknowledgableWatermark acknowledgableWatermark) {
    if (!_started.get() && _autoStart) {
      start();
    }
    maybeAbort();
    String source = acknowledgableWatermark.getCheckpointableWatermark().getSource();
    Deque<AcknowledgableWatermark> sourceWatermarks = _watermarksMap.get(source);
    if (sourceWatermarks == null) {
      sourceWatermarks = new ConcurrentLinkedDeque<>();
      _watermarksMap.put(source, sourceWatermarks);
    }
    sourceWatermarks.add(acknowledgableWatermark);
    _watermarksInserted.mark();
  }

  private void maybeAbort() throws RuntimeException {
    if (_abort.get()) {
      throw new RuntimeException("Aborting Watermark tracking");
    }
  }

  /**
   * Check if the memory footprint of the data structure is within bounds
   */
  private void checkStability() {
    if ((_watermarksInserted.getCount() - _watermarksSwept.getCount()) > _watermarkLagThreshold) {
      log.error("Setting abort flag for Watermark tracking because the lag between the "
          + "watermarksInserted: {} and watermarksSwept: {} is greater than the threshold: {}",
          _watermarksInserted.getCount(), _watermarksSwept.getCount(), _watermarkLagThreshold);
      _abort.set(true);
    }
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
   * Schedule the sweeper and stability checkers
   */
  public synchronized void start() {
    if (!_started.get()) {
      _executorService = new ScheduledThreadPoolExecutor(1,
          ExecutorsUtils.newThreadFactory(Optional.of(LoggerFactory.getLogger(FineGrainedWatermarkTracker.class))));
      _executorService.scheduleAtFixedRate(_sweeper, 0, _sweepIntervalMillis, TimeUnit.MILLISECONDS);
      _executorService.scheduleAtFixedRate(_stabilityChecker, 0, _stabilityCheckIntervalMillis, TimeUnit.MILLISECONDS);
    }
    _started.set(true);
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (_executorService != null) {
        _executorService.shutdown();
      }
    } finally {
      _closer.close();
    }
  }

  /**
   * A helper method to garbage collect acknowledged watermarks
   * @return number of elements collected
   */
  @VisibleForTesting
  synchronized int sweep() {
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
    _watermarksSwept.mark(swept);
    return swept;
  }




  private final Runnable _sweeper;
  private final Runnable _stabilityChecker;


  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    _metricContext = _closer
        .register(Instrumented.newContextFromReferenceContext(_metricContext, tags, Optional.<String>absent()));
    regenerateMetrics();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    _metricContext = context;
    regenerateMetrics();
  }

  /** Default with no additional tags */
  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    return _metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return _instrumentationEnabled;
  }

  /**
   * Generates metrics for the instrumentation of this class.
   */
  protected void regenerateMetrics() {
    // Set up the metrics that are enabled regardless of instrumentation
    _watermarksInserted = _metricContext.meter(WATERMARKS_INSERTED_METER);
    _watermarksSwept = _metricContext.meter(WATERMARKS_SWEPT_METER);
  }
}
