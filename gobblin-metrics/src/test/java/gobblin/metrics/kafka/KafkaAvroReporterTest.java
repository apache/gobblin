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

import org.apache.avro.specific.SpecificDatumReader;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import kafka.consumer.ConsumerIterator;

import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.MetricReportUtils;


/**
 * Test for KafkaAvroReporter
 * Extends KafkaReporterTest and just redefines the builder and the metrics deserializer
 *
 * @author ibuenros
 */
@Test(groups = {"gobblin.metrics"})
public class KafkaAvroReporterTest extends KafkaReporterTest {

  private SpecificDatumReader<MetricReport> reader;

  public KafkaAvroReporterTest(String topic)
      throws IOException, InterruptedException {
    super(topic);

    reader = null;

  }

  public KafkaAvroReporterTest() throws IOException, InterruptedException {
    this("KafkaAvroReporterTest");
  }

  @Override
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilder(MetricRegistry registry) {
    return KafkaAvroReporter.forRegistry(registry);
  }

  @Override
  public KafkaReporter.Builder<?> getBuilderFromContext(MetricContext context) {
    return KafkaAvroReporter.forContext(context);
  }

  @BeforeClass
  public void setup() {
    reader = new SpecificDatumReader<MetricReport>(MetricReport.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected MetricReport nextReport(ConsumerIterator<byte[], byte[]> it)
      throws IOException {
    Assert.assertTrue(it.hasNext());
    return MetricReportUtils.deserializeReportFromAvroSerialization(new MetricReport(), it.next().message());
  }
}
