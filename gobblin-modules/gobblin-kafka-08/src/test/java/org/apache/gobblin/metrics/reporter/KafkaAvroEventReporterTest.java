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

import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.MetricContext;
import gobblin.metrics.reporter.util.EventUtils;
import gobblin.metrics.kafka.KafkaAvroEventReporter;
import gobblin.metrics.kafka.KafkaEventReporter;
import gobblin.metrics.kafka.KafkaPusher;


@Test(groups = {"gobblin.metrics"})
public class KafkaAvroEventReporterTest extends KafkaEventReporterTest {

  @Override
  public KafkaEventReporter.Builder<? extends KafkaEventReporter.Builder> getBuilder(MetricContext context,
      KafkaPusher pusher) {
    return KafkaAvroEventReporter.forContext(context).withKafkaPusher(pusher);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected GobblinTrackingEvent nextEvent(Iterator<byte[]> it)
      throws IOException {
    Assert.assertTrue(it.hasNext());
    return EventUtils.deserializeReportFromAvroSerialization(new GobblinTrackingEvent(), it.next());
  }
}
