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

package org.apache.gobblin.runtime.job_monitor;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.event.sla.SlaEventKeys;
import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.kafka.HighLevelConsumerTest;
import org.apache.gobblin.util.Either;


public class SLAEventKafkaJobMonitorTest {

  private URI templateURI;
  private Config superConfig;

  public SLAEventKafkaJobMonitorTest() throws Exception {
    this.templateURI = new URI("/templates/uri");
    this.superConfig = HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX));
  }

  @Test
  public void testParseJobSpec() throws Exception {

    SLAEventKafkaJobMonitor monitor =
        new SLAEventKafkaJobMonitor("topic", null, new URI("/base/URI"),
            HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX)),
            new NoopSchemaVersionWriter(), Optional.<Pattern>absent(), Optional.<Pattern>absent(), this.templateURI,
            ImmutableMap.of("metadataKey1", "key1"));

    monitor.buildMetricsContextAndMetrics();

    GobblinTrackingEvent event = createSLAEvent("DatasetPublish", new URI("/data/myDataset"),
        ImmutableMap.of("metadataKey1","value1","key1","value2"));
    Collection<Either<JobSpec, URI>> jobSpecs = monitor.parseJobSpec(event);

    Assert.assertEquals(jobSpecs.size(), 1);
    JobSpec jobSpec = (JobSpec) jobSpecs.iterator().next().get();
    Assert.assertEquals(jobSpec.getUri(), new URI("/base/URI/data/myDataset"));
    Assert.assertEquals(jobSpec.getTemplateURI().get(), templateURI);
    // should insert configuration from metadata
    Assert.assertEquals(jobSpec.getConfig().getString("key1"), "value1");

    monitor.shutdownMetrics();
  }

  @Test
  public void testFilterByName() throws Exception {

    SLAEventKafkaJobMonitor monitor =
        new SLAEventKafkaJobMonitor("topic", null, new URI("/base/URI"),
            HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX)),
            new NoopSchemaVersionWriter(), Optional.<Pattern>absent(), Optional.of(Pattern.compile("^accept.*")),
            this.templateURI, ImmutableMap.<String, String>of());

    monitor.buildMetricsContextAndMetrics();

    GobblinTrackingEvent event;
    Collection<Either<JobSpec, URI>> jobSpecs;

    event = createSLAEvent("acceptthis", new URI("/data/myDataset"), Maps.<String, String>newHashMap());
    jobSpecs = monitor.parseJobSpec(event);
    Assert.assertEquals(jobSpecs.size(), 1);
    Assert.assertEquals(monitor.getRejectedEvents().getCount(), 0);

    event = createSLAEvent("donotacceptthis", new URI("/data/myDataset"), Maps.<String, String>newHashMap());
    jobSpecs = monitor.parseJobSpec(event);
    Assert.assertEquals(jobSpecs.size(), 0);
    Assert.assertEquals(monitor.getRejectedEvents().getCount(), 1);

    monitor.shutdownMetrics();
  }

  @Test
  public void testFilterByDatasetURN() throws Exception {
    Properties props = new Properties();
    props.put(SLAEventKafkaJobMonitor.TEMPLATE_KEY, templateURI.toString());
    props.put(SLAEventKafkaJobMonitor.DATASET_URN_FILTER_KEY, "^/accept.*");
    Config config = ConfigFactory.parseProperties(props).withFallback(superConfig);

    SLAEventKafkaJobMonitor monitor =
        new SLAEventKafkaJobMonitor("topic", null, new URI("/base/URI"),
            HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX)),
            new NoopSchemaVersionWriter(), Optional.of(Pattern.compile("^/accept.*")), Optional.<Pattern>absent(),
            this.templateURI, ImmutableMap.<String, String>of());

    monitor.buildMetricsContextAndMetrics();

    GobblinTrackingEvent event;
    Collection<Either<JobSpec, URI>> jobSpecs;

    event = createSLAEvent("event", new URI("/accept/myDataset"), Maps.<String, String>newHashMap());
    jobSpecs = monitor.parseJobSpec(event);
    Assert.assertEquals(jobSpecs.size(), 1);
    Assert.assertEquals(monitor.getRejectedEvents().getCount(), 0);

    event = createSLAEvent("event", new URI("/reject/myDataset"), Maps.<String, String>newHashMap());
    jobSpecs = monitor.parseJobSpec(event);
    Assert.assertEquals(jobSpecs.size(), 0);
    Assert.assertEquals(monitor.getRejectedEvents().getCount(), 1);

    monitor.shutdownMetrics();
  }

  @Test
  public void testFactory() throws Exception {

    Pattern urnFilter = Pattern.compile("filter");
    Pattern nameFilter = Pattern.compile("filtername");

    Map<String, String> configMap = ImmutableMap.<String, String>builder()
        .put(SLAEventKafkaJobMonitor.DATASET_URN_FILTER_KEY, urnFilter.pattern())
        .put(SLAEventKafkaJobMonitor.EVENT_NAME_FILTER_KEY, nameFilter.pattern())
        .put(SLAEventKafkaJobMonitor.TEMPLATE_KEY, "template")
        .put(SLAEventKafkaJobMonitor.EXTRACT_KEYS + ".key1", "value1")
        .put(SLAEventKafkaJobMonitor.BASE_URI_KEY, "uri")
        .put(SLAEventKafkaJobMonitor.TOPIC_KEY, "topic")
        .put(SLAEventKafkaJobMonitor.SCHEMA_VERSION_READER_CLASS, FixedSchemaVersionWriter.class.getName()).build();
    Config config = ConfigFactory.parseMap(configMap).
        withFallback(HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX)));

    SLAEventKafkaJobMonitor monitor =
        (SLAEventKafkaJobMonitor) (new SLAEventKafkaJobMonitor.Factory()).forConfig(config, null);

    Assert.assertEquals(monitor.getUrnFilter().get().pattern(), urnFilter.pattern());
    Assert.assertEquals(monitor.getNameFilter().get().pattern(), nameFilter.pattern());
    Assert.assertEquals(monitor.getTemplate(), new URI("template"));
    Assert.assertEquals(monitor.getExtractKeys().size(), 1);
    Assert.assertEquals(monitor.getExtractKeys().get("key1"), "value1");
    Assert.assertEquals(monitor.getBaseURI(), new URI("uri"));
    Assert.assertEquals(monitor.getTopic(), "topic");
    Assert.assertEquals(monitor.getVersionWriter().getClass(), FixedSchemaVersionWriter.class);
  }

  private GobblinTrackingEvent createSLAEvent(String name, URI urn, Map<String, String> additionalMetadata) {
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(SlaEventKeys.DATASET_URN_KEY, urn.toString());
    metadata.putAll(additionalMetadata);
    return new GobblinTrackingEvent(0L, "namespace", name, metadata);
  }

}
