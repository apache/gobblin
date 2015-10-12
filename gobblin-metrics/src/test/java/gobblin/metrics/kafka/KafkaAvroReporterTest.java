/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */


package gobblin.metrics.kafka;

import java.io.IOException;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.util.MetricReportUtils;


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
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilder(MetricRegistry registry, KafkaPusher pusher) {
    return KafkaAvroReporter.Factory.forRegistry(registry).withKafkaPusher(pusher);
  }

  @Override
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilderFromContext(MetricContext context, KafkaPusher pusher) {
    return KafkaAvroReporter.Factory.forContext(context).withKafkaPusher(pusher);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected MetricReport nextReport(Iterator<byte[]> it)
      throws IOException {
    Assert.assertTrue(it.hasNext());
    return MetricReportUtils.deserializeReportFromAvroSerialization(new MetricReport(), it.next());
  }
}
