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

package gobblin.runtime.job_monitor;

import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.Metric;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.util.AvroBinarySerializer;
import gobblin.metrics.reporter.util.AvroSerializer;
import gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import gobblin.metrics.reporter.util.SchemaVersionWriter;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.kafka.HighLevelConsumerTest;
import gobblin.util.Either;


public class KafkaAvroJobMonitorTest {

  @Test
  public void testSimple() throws Exception {

    TestKafkaAvroJobMonitor monitor =
        new TestKafkaAvroJobMonitor(GobblinTrackingEvent.SCHEMA$, new NoopSchemaVersionWriter());
    monitor.buildMetricsContextAndMetrics();

    AvroSerializer<GobblinTrackingEvent> serializer =
        new AvroBinarySerializer<>(GobblinTrackingEvent.SCHEMA$, new NoopSchemaVersionWriter());

    GobblinTrackingEvent event = new GobblinTrackingEvent(0L, "namespace", "event", Maps.<String, String>newHashMap());
    Collection<Either<JobSpec, URI>> results = monitor.parseJobSpec(serializer.serializeRecord(event));
    Assert.assertEquals(results.size(), 1);
    Assert.assertEquals(monitor.events.size(), 1);
    Assert.assertEquals(monitor.events.get(0), event);

    monitor.shutdownMetrics();
  }

  @Test
  public void testWrongSchema() throws Exception {

    TestKafkaAvroJobMonitor monitor =
        new TestKafkaAvroJobMonitor(GobblinTrackingEvent.SCHEMA$, new NoopSchemaVersionWriter());
    monitor.buildMetricsContextAndMetrics();

    AvroSerializer<MetricReport> serializer =
        new AvroBinarySerializer<>(MetricReport.SCHEMA$, new NoopSchemaVersionWriter());

    MetricReport event = new MetricReport(Maps.<String, String>newHashMap(), 0L, Lists.<Metric>newArrayList());
    Collection<Either<JobSpec, URI>> results = monitor.parseJobSpec(serializer.serializeRecord(event));

    Assert.assertEquals(results.size(), 0);
    Assert.assertEquals(monitor.events.size(), 0);
    Assert.assertEquals(monitor.getMessageParseFailures().getCount(), 1);

    monitor.shutdownMetrics();
  }

  @Test
  public void testUsingSchemaVersion() throws Exception {

    TestKafkaAvroJobMonitor monitor =
        new TestKafkaAvroJobMonitor(GobblinTrackingEvent.SCHEMA$, new FixedSchemaVersionWriter());
    monitor.buildMetricsContextAndMetrics();

    AvroSerializer<GobblinTrackingEvent> serializer =
        new AvroBinarySerializer<>(GobblinTrackingEvent.SCHEMA$, new FixedSchemaVersionWriter());

    GobblinTrackingEvent event = new GobblinTrackingEvent(0L, "namespace", "event", Maps.<String, String>newHashMap());
    Collection<Either<JobSpec, URI>> results = monitor.parseJobSpec(serializer.serializeRecord(event));
    Assert.assertEquals(results.size(), 1);
    Assert.assertEquals(monitor.events.size(), 1);
    Assert.assertEquals(monitor.events.get(0), event);

    monitor.shutdownMetrics();
  }

  @Test
  public void testWrongSchemaVersionWriter() throws Exception {
    TestKafkaAvroJobMonitor monitor =
        new TestKafkaAvroJobMonitor(GobblinTrackingEvent.SCHEMA$, new NoopSchemaVersionWriter());
    monitor.buildMetricsContextAndMetrics();

    AvroSerializer<GobblinTrackingEvent> serializer =
        new AvroBinarySerializer<>(GobblinTrackingEvent.SCHEMA$, new FixedSchemaVersionWriter());

    GobblinTrackingEvent event = new GobblinTrackingEvent(0L, "namespace", "event", Maps.<String, String>newHashMap());
    Collection<Either<JobSpec, URI>> results = monitor.parseJobSpec(serializer.serializeRecord(event));
    Assert.assertEquals(results.size(), 0);
    Assert.assertEquals(monitor.events.size(), 0);
    Assert.assertEquals(monitor.getMessageParseFailures().getCount(), 1);

    monitor.shutdownMetrics();
  }

  private class TestKafkaAvroJobMonitor extends KafkaAvroJobMonitor<GobblinTrackingEvent> {

    private List<GobblinTrackingEvent> events = Lists.newArrayList();

    public TestKafkaAvroJobMonitor(Schema schema, SchemaVersionWriter<?> versionWriter) {
      super("dummy", null, HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX)),
          schema, versionWriter);
    }

    @Override
    public Collection<Either<JobSpec, URI>> parseJobSpec(GobblinTrackingEvent message) {
      this.events.add(message);
      return Lists.newArrayList(Either.<JobSpec, URI>left(JobSpec.builder(message.getName()).build()));
    }
  }


}
