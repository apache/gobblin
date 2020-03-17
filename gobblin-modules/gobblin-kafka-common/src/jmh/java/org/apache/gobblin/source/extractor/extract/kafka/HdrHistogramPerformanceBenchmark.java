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
package org.apache.gobblin.source.extractor.extract.kafka;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.HdrHistogram.Histogram;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import lombok.extern.slf4j.Slf4j;


/**
 * A micro-benchmark to measure the time taken to serialize a {@link Histogram} instance to its String representation. The
 * benchmark uses a Random number generator to generate values according to a Uniform Distribution, an adversarial pattern
 * for a Histogram that is likely to produce more count buckets in comparison with a skewed distribution. The benchmark
 * provides an upper bound on memory footprint of the histogram, serialization time, as well as the size of the
 * serialized representation.
 */
@Warmup (iterations = 3)
@Measurement (iterations = 10)
@BenchmarkMode (value = Mode.AverageTime)
@Fork (value = 1)
@OutputTimeUnit (TimeUnit.MILLISECONDS)
@Slf4j
public class HdrHistogramPerformanceBenchmark {

  @State (value = Scope.Benchmark)
  public static class HistogramState {
    private static long MIN_VALUE = 1;
    private static long MAX_VALUE = TimeUnit.HOURS.toMillis(24);

    private Histogram histogram1;
    private Histogram histogram2;
    private Histogram histogram3;
    private Histogram histogram4;

    private final RandomDataGenerator random = new RandomDataGenerator();

    @Setup (value = Level.Iteration)
    public void setUp() {
      this.histogram1 = buildHistogram(1000000);
      this.histogram2 = buildHistogram(2000000);
      this.histogram3 = buildHistogram(4000000);
      this.histogram4 = buildHistogram(10000000);
    }

    private Histogram buildHistogram(int size) {
      Histogram histogram = new Histogram(MIN_VALUE, MAX_VALUE, 3);
      IntStream.range(0, size).mapToLong(i -> random.nextLong(MIN_VALUE, MAX_VALUE))
          .forEachOrdered(histogram::recordValue);
      System.out.println("Estimated memory footprint of histogram is: " + histogram.getEstimatedFootprintInBytes());
      return histogram;
    }

    @TearDown (value = Level.Iteration)
    public void tearDown() {
      this.histogram1.reset();
      this.histogram2.reset();
      this.histogram3.reset();
      this.histogram4.reset();
    }
  }

  @Benchmark
  public String trackHistogram1MToStringConversion(HistogramState histogramState) {
    String histogramString = KafkaExtractorStatsTracker.convertHistogramToString(histogramState.histogram1);
    System.out.println("Histogram serialized string size: " + histogramString.length());
    return histogramString;
  }

  @Benchmark
  public String trackHistogram2MToStringConversion(HistogramState histogramState) {
    String histogramString = KafkaExtractorStatsTracker.convertHistogramToString(histogramState.histogram2);
    System.out.println("Histogram serialized string size: " + histogramString.length());
    return histogramString;
  }

  @Benchmark
  public String trackHistogram4MToStringConversion(HistogramState histogramState) {
    String histogramString = KafkaExtractorStatsTracker.convertHistogramToString(histogramState.histogram3);
    System.out.println("Histogram serialized string size: " + histogramString.length());
    return histogramString;
  }

  @Benchmark
  public String trackHistogram10MToStringConversion(HistogramState histogramState) {
    String histogramString = KafkaExtractorStatsTracker.convertHistogramToString(histogramState.histogram4);
    System.out.println("Histogram serialized string size: " + histogramString.length());
    return histogramString;
  }

  @Benchmark
  public Histogram trackMergeHistogram(HistogramState histogramState) {
    Histogram histogram = new Histogram(histogramState.MIN_VALUE, histogramState.MAX_VALUE, 3);
    histogram.add(histogramState.histogram1);
    histogram.add(histogramState.histogram2);
    histogram.add(histogramState.histogram3);
    histogram.add(histogramState.histogram4);
    return histogram;
  }

  @Benchmark
  public Histogram trackBuildHistogram(HistogramState histogramState) {
    Histogram histogram = new Histogram(histogramState.MIN_VALUE, histogramState.MAX_VALUE, 3);
    return histogram;
  }

  @Benchmark
  public void trackResetHistogram(HistogramState histogramState, Blackhole blackhole) {
    int dummyVal = 1;
    histogramState.histogram4.reset();
    blackhole.consume(dummyVal);
  }

  public static void main(String[] args) throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(HdrHistogramPerformanceBenchmark.class.getSimpleName())
        .warmupIterations(3)
        .measurementIterations(10);
    new Runner(opt.build()).run();
  }
}
