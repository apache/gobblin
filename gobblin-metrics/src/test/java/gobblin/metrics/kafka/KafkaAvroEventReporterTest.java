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

import org.testng.Assert;

import kafka.consumer.ConsumerIterator;

import gobblin.metrics.Event;
import gobblin.metrics.MetricContext;
import gobblin.metrics.reporter.util.EventUtils;


public class KafkaAvroEventReporterTest extends KafkaEventReporterTest {

  public KafkaAvroEventReporterTest(String topic)
      throws IOException, InterruptedException {
    super(topic);
  }

  public KafkaAvroEventReporterTest()
      throws IOException, InterruptedException {
    this("KafkaAvroEventReporterTest");
  }

  @Override
  public KafkaEventReporter.Builder<? extends KafkaEventReporter.Builder> getBuilder(MetricContext context) {
    return KafkaAvroEventReporter.forContext(context);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Event nextEvent(ConsumerIterator<byte[], byte[]> it)
      throws IOException {
    Assert.assertTrue(it.hasNext());
    return EventUtils.deserializeReportFromAvroSerialization(new Event(), it.next().message());
  }
}
