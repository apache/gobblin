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

package org.apache.gobblin.metrics.opentelemetry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.InMemoryOpenTelemetryMetrics;

/** Tests for OpenTelemetry metrics implementation, & its associated factory classes {@link OpenTelemetryMetricFactory}
 * specifically for {@link OpenTelemetryLongCounter} and {@link OpenTelemetryDoubleHistogram}.
 * These tests validate the object creation through factory classes {@link OpenTelemetryMetricFactory#LONG_COUNTER_FACTORY}
 * & {@link OpenTelemetryMetricFactory#DOUBLE_HISTOGRAM_FACTORY} ,correct recording of metrics with and without
 * additional attributes.
 */
public class OpenTelemetryMetricTest {

  private InMemoryOpenTelemetryMetrics inMemoryOpenTelemetryMetrics;
  private final String testMeterGroupName = "testMeterGroup";
  private final Attributes baseAttributes = Attributes.builder()
      .put("dim1", "val1")
      .put("dim2", "val2")
      .build();

  @BeforeMethod
  public void setUp() {
    inMemoryOpenTelemetryMetrics = InMemoryOpenTelemetryMetrics.getInstance(new State());
  }

  @Test
  public void testOpenTelemetryLongCounter() {
    String metricName = "testLongCounter";
    OpenTelemetryLongCounter longCounter = OpenTelemetryMetricFactory.LONG_COUNTER_FACTORY.newMetric(metricName,
        "testLongCounterDescription", "1", baseAttributes, inMemoryOpenTelemetryMetrics.getMeter(testMeterGroupName));
    longCounter.add(20, Attributes.builder().put("dim3", "val3").build());
    Collection<MetricData> metrics = inMemoryOpenTelemetryMetrics.metricReader.collectAllMetrics();
    Assert.assertEquals(metrics.size(), 1);
    Map<String, MetricData > metricsByName = metrics.stream().collect(Collectors.toMap(MetricData::getName, metricData -> metricData));
    MetricData metricData = metricsByName.get(metricName);
    List<LongPointData> dataPoints = new ArrayList<>(metricData.getLongSumData().getPoints());
    Assert.assertEquals(dataPoints.size(), 1);
    Assert.assertEquals(dataPoints.get(0).getValue(), 20);
    Assert.assertEquals(dataPoints.get(0).getAttributes().get(AttributeKey.stringKey("dim1")), "val1");
    Assert.assertEquals(dataPoints.get(0).getAttributes().get(AttributeKey.stringKey("dim2")), "val2");
    Assert.assertEquals(dataPoints.get(0).getAttributes().get(AttributeKey.stringKey("dim3")), "val3");
  }

  @Test
  public void testOpenTelemetryLongCounterWithoutAdditionalAttributes() {
    String metricName = "testLongCounterWithoutAdditionalAttributes";
    OpenTelemetryLongCounter longCounter = OpenTelemetryMetricFactory.LONG_COUNTER_FACTORY.newMetric(metricName,
        "testLongCounterDescription", "1", baseAttributes, inMemoryOpenTelemetryMetrics.getMeter(testMeterGroupName));
    longCounter.add(10);
    Collection<MetricData> metrics = inMemoryOpenTelemetryMetrics.metricReader.collectAllMetrics();
    Assert.assertEquals(metrics.size(), 1);
    Map<String, MetricData > metricsByName = metrics.stream().collect(Collectors.toMap(MetricData::getName, metricData -> metricData));
    MetricData metricData = metricsByName.get(metricName);
    List<LongPointData> dataPoints = new ArrayList<>(metricData.getLongSumData().getPoints());
    Assert.assertEquals(dataPoints.size(), 1);
    Assert.assertEquals(dataPoints.get(0).getValue(), 10);
    Assert.assertEquals(dataPoints.get(0).getAttributes().get(AttributeKey.stringKey("dim1")), "val1");
    Assert.assertEquals(dataPoints.get(0).getAttributes().get(AttributeKey.stringKey("dim2")), "val2");
    Assert.assertNull(dataPoints.get(0).getAttributes().get(AttributeKey.stringKey("dim3")),
        "Additional attribute dim3 should not be present when not provided");
  }

  @Test
  public void testOpenTelemetryDoubleHistogram() {
    String metricName = "testDoubleHistogram";
    OpenTelemetryDoubleHistogram doubleHistogram = OpenTelemetryMetricFactory.DOUBLE_HISTOGRAM_FACTORY.newMetric(metricName,
        "testDoubleHistogramDescription", "s", baseAttributes, inMemoryOpenTelemetryMetrics.getMeter(testMeterGroupName));
    doubleHistogram.record(5.0, Attributes.builder().put("dim3", "val3").build());
    doubleHistogram.record(10.0, Attributes.builder().put("dim3", "val3").build());
    Collection<MetricData> metrics = inMemoryOpenTelemetryMetrics.metricReader.collectAllMetrics();
    Assert.assertEquals(metrics.size(), 1);
    Map<String, MetricData > metricsByName = metrics.stream().collect(Collectors.toMap(MetricData::getName, metricData -> metricData));
    MetricData metricData = metricsByName.get(metricName);
    List<HistogramPointData> dataPoints = new ArrayList<>(metricData.getHistogramData().getPoints());
    Assert.assertEquals(dataPoints.size(), 1);
    Assert.assertEquals(dataPoints.get(0).getSum(), 15.0, "Sum should match recorded value");
  }

}
