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

package org.apache.gobblin.converter;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.Metric;
import org.apache.gobblin.metrics.MetricReport;
import org.apache.gobblin.metrics.reporter.util.AvroBinarySerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import org.apache.gobblin.util.AvroUtils;

public class GobblinMetricsPinotFlattenerConverterTest {

  @Test
  public void test() throws Exception {

    MetricReport metricReport = new MetricReport();
    metricReport.setTags(ImmutableMap.of("tag", "value", "tag2", "value2"));
    metricReport.setTimestamp(10L);
    metricReport.setMetrics(Lists.newArrayList(new Metric("metric", 1.0), new Metric("metric2", 2.0)));

    AvroSerializer<MetricReport> serializer =
        new AvroBinarySerializer<>(MetricReport.SCHEMA$, new NoopSchemaVersionWriter());
    serializer.serializeRecord(metricReport);

    Schema metricReportUtf8 = new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("MetricReport.avsc"));
    GenericRecord genericRecordMetric = AvroUtils.slowDeserializeGenericRecord(serializer.serializeRecord(metricReport), metricReportUtf8);

    GobblinMetricsPinotFlattenerConverter converter = new GobblinMetricsPinotFlattenerConverter();

    Schema outputSchema = converter.convertSchema(MetricReport.SCHEMA$, new WorkUnitState());
    Iterable<GenericRecord> converted = converter.convertRecord(outputSchema, genericRecordMetric, new WorkUnitState());
    List<GenericRecord> convertedList = Lists.newArrayList(converted);

    Assert.assertEquals(convertedList.size(), 2);

    Assert.assertEquals(Sets.newHashSet((List<Utf8>) convertedList.get(0).get("tags")),
        Sets.newHashSet("tag:value", "tag2:value2"));
    Assert.assertEquals(convertedList.get(0).get("timestamp"), 10L);
    Assert.assertEquals(convertedList.get(0).get("metricName").toString(), "metric");
    Assert.assertEquals(convertedList.get(0).get("metricValue"), 1.0);

    Assert.assertEquals(Sets.newHashSet((List<Utf8>) convertedList.get(1).get("tags")),
        Sets.newHashSet("tag:value", "tag2:value2"));
    Assert.assertEquals(convertedList.get(1).get("timestamp"), 10L);
    Assert.assertEquals(convertedList.get(1).get("metricName").toString(), "metric2");
    Assert.assertEquals(convertedList.get(1).get("metricValue"), 2.0);

  }

}
