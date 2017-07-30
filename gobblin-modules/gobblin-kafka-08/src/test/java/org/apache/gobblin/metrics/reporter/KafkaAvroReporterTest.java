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

package gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.util.MetricReportUtils;
import gobblin.metrics.kafka.KafkaAvroReporter;
import gobblin.metrics.kafka.KafkaPusher;
import gobblin.metrics.kafka.KafkaReporter;


/**
 * Test for KafkaAvroReporter
 * Extends KafkaReporterTest and just redefines the builder and the metrics deserializer
 *
 * @author ibuenros
 */
@Test(groups = {"gobblin.metrics"})
public class KafkaAvroReporterTest extends KafkaReporterTest {

  public KafkaAvroReporterTest(String topic)
      throws IOException, InterruptedException {
    super();
  }

  public KafkaAvroReporterTest() throws IOException, InterruptedException {
    this("KafkaAvroReporterTest");
  }

  @Override
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilder(KafkaPusher pusher) {
    return KafkaAvroReporter.BuilderFactory.newBuilder().withKafkaPusher(pusher);
  }

  @Override
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilderFromContext(KafkaPusher pusher) {
    return KafkaAvroReporter.BuilderFactory.newBuilder().withKafkaPusher(pusher);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected MetricReport nextReport(Iterator<byte[]> it)
      throws IOException {
    Assert.assertTrue(it.hasNext());
    return MetricReportUtils.deserializeReportFromAvroSerialization(new MetricReport(), it.next());
  }
}
