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
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.source.extractor.DefaultCheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.util.ExecutorsUtils;


@Warmup(iterations = 3)
@Measurement(iterations = 10)
@org.openjdk.jmh.annotations.Fork(value = 3)
@BenchmarkMode(value = Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FineGrainedWatermarkTrackerBenchmark {
  @State(value = Scope.Group)
  public static class TrackerState {
    private FineGrainedWatermarkTracker _watermarkTracker;
    private ScheduledExecutorService _executorService;
    private long _index;
    private final Random _random = new Random();

    @Setup
    public void setup() throws Exception {
      Properties properties = new Properties();
      Config config = ConfigFactory.parseProperties(properties);
      _watermarkTracker = new FineGrainedWatermarkTracker(config);
      _index = 0;
      _executorService = new ScheduledThreadPoolExecutor(40,
          ExecutorsUtils.newThreadFactory(Optional.of(LoggerFactory.getLogger(FineGrainedWatermarkTrackerBenchmark.class))));
    }

    @TearDown
    public void tearDown() throws IOException {
      _watermarkTracker.close();
      _executorService.shutdown();
    }
  }

  @Benchmark
  @Group("trackImmediate")
  public void trackImmediateAcks(Control control, TrackerState trackerState) throws Exception {
    if (!control.stopMeasurement) {
      AcknowledgableWatermark wmark = new AcknowledgableWatermark(new DefaultCheckpointableWatermark(
          "0", new LongWatermark(trackerState._index)));
      trackerState._watermarkTracker.track(wmark);
      trackerState._index++;
      wmark.ack();
    }
  }

  @Benchmark
  @Group("trackDelayed")
  public void trackWithDelayedAcks(Control control, TrackerState trackerState) throws Exception {
    if (!control.stopMeasurement) {
      final AcknowledgableWatermark wmark = new AcknowledgableWatermark(new DefaultCheckpointableWatermark(
          "0", new LongWatermark(trackerState._index)));
      trackerState._watermarkTracker.track(wmark);
      trackerState._index++;
      int delay = trackerState._random.nextInt(10);
      trackerState._executorService.schedule(new Runnable() {
        @Override
        public void run() {
          wmark.ack();
        }
      }, delay, TimeUnit.MILLISECONDS);
    }
  }

  @Benchmark
  @Group("scheduledDelayed")
  public void scheduledDelayedAcks(Control control, TrackerState trackerState) throws Exception {
    if (!control.stopMeasurement) {
      final AcknowledgableWatermark wmark = new AcknowledgableWatermark(new DefaultCheckpointableWatermark(
          "0", new LongWatermark(trackerState._index)));
      trackerState._index++;
      int delay = trackerState._random.nextInt(10);
      trackerState._executorService.schedule(new Runnable() {
        @Override
        public void run() {
          wmark.ack();
        }
      }, delay, TimeUnit.MILLISECONDS);
    }
  }

  @Benchmark
  @Group("scheduledNoRandom")
  public void scheduledNoRandomDelayedAcks(Control control, TrackerState trackerState) throws Exception {
    if (!control.stopMeasurement) {
      final AcknowledgableWatermark wmark = new AcknowledgableWatermark(new DefaultCheckpointableWatermark(
          "0", new LongWatermark(trackerState._index)));
      trackerState._index++;
      int delay = 10;
      trackerState._executorService.schedule(new Runnable() {
        @Override
        public void run() {
          wmark.ack();
        }
      }, delay, TimeUnit.MILLISECONDS);
    }
  }


}