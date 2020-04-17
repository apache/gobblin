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

package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.commons.codec.digest.DigestUtils;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.metrics.MetricReport;
import org.apache.gobblin.metrics.kafka.KafkaAvroReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaReporter;
import org.apache.gobblin.metrics.kafka.Pusher;
import org.apache.gobblin.metrics.reporter.util.MetricReportUtils;


/**
 * Test for KafkaAvroReporter that is configured with a {@link KafkaAvroSchemaRegistry}.
 * Extends KafkaAvroReporterTest and just redefines the builder and the metrics deserializer
 *
 * @author Sudarshan Vasudevan
 */
@Test(groups = {"gobblin.metrics"})
public class KafkaAvroReporterWithSchemaRegistryTest extends KafkaAvroReporterTest {
  private static final int SCHEMA_ID_LENGTH_BYTES = 20;

  private String schemaId;
  private KafkaAvroSchemaRegistry schemaRegistry;

  public KafkaAvroReporterWithSchemaRegistryTest(String topic)
      throws IOException, InterruptedException {
    super();
    this.schemaId = getSchemaId();
    this.schemaRegistry = getMockSchemaRegistry();
  }

  public KafkaAvroReporterWithSchemaRegistryTest()
      throws IOException, InterruptedException {
    this("KafkaAvroReporterTestWithSchemaRegistry");
    this.schemaId = getSchemaId();
  }

  private KafkaAvroSchemaRegistry getMockSchemaRegistry() throws IOException {
    KafkaAvroSchemaRegistry registry  = Mockito.mock(KafkaAvroSchemaRegistry.class);
    Mockito.when(registry.getSchemaIdLengthByte()).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) {
        return KafkaAvroReporterWithSchemaRegistryTest.SCHEMA_ID_LENGTH_BYTES;
      }
    });
    Mockito.when(registry.getSchemaByKey(Mockito.anyString())).thenAnswer(new Answer<Schema>() {
      @Override
      public Schema answer(InvocationOnMock invocation) {
        return MetricReport.SCHEMA$;
      }
    });
    return registry;
  }

  private String getSchemaId() throws IOException {
    Schema schema =
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("MetricReport.avsc"));
    return DigestUtils.sha1Hex(schema.toString().getBytes());
  }

  @Override
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilder(Pusher pusher) {
    return KafkaAvroReporter.BuilderFactory.newBuilder().withKafkaPusher(pusher).withSchemaRegistry(this.schemaRegistry).withSchemaId(schemaId);
  }

  @Override
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilderFromContext(Pusher pusher) {
    return KafkaAvroReporter.BuilderFactory.newBuilder().withKafkaPusher(pusher).withSchemaRegistry(this.schemaRegistry).withSchemaId(schemaId);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected MetricReport nextReport(Iterator<byte[]> it)
      throws IOException {
    Assert.assertTrue(it.hasNext());
    return MetricReportUtils.deserializeReportFromAvroSerialization(new MetricReport(), it.next(), this.schemaId);
  }
}
