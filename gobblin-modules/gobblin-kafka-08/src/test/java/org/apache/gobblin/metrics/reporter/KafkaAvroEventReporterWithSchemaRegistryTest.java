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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaEventReporter;


public class KafkaAvroEventReporterWithSchemaRegistryTest {

  private final Map<String, Schema> schemas = Maps.newConcurrentMap();

  @Test
  public void test() throws Exception {
    testHelper(false);
  }

  @Test
  public void testWithSchemaId() throws IOException {
    testHelper(true);
  }

  private String register(Schema schema) {
    String id = DigestUtils.sha1Hex(schema.toString().getBytes());
    this.schemas.put(id, schema);
    return id;
  }

  private void testHelper(boolean isSchemaIdEnabled) throws IOException {
    MetricContext context = MetricContext.builder("context").build();

    MockKafkaPusher pusher = new MockKafkaPusher();
    KafkaAvroSchemaRegistry registry = Mockito.mock(KafkaAvroSchemaRegistry.class);
    KafkaAvroEventReporter.Builder builder = KafkaAvroEventReporter.forContext(context).withKafkaPusher(pusher).withSchemaRegistry(registry);

    Schema schema =
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc"));
    String schemaId = DigestUtils.sha1Hex(schema.toString().getBytes());

    if (!isSchemaIdEnabled) {
      Mockito.when(registry.register(Mockito.any(Schema.class))).thenAnswer(new Answer<String>() {
        @Override
        public String answer(InvocationOnMock invocation)
            throws Throwable {
          return register((Schema) invocation.getArguments()[0]);
        }
      });
      Mockito.when(registry.register(Mockito.any(Schema.class), Mockito.anyString())).thenAnswer(new Answer<String>() {
        @Override
        public String answer(InvocationOnMock invocation)
            throws Throwable {
          return register((Schema) invocation.getArguments()[0]);
        }
      });
    } else {
      builder.withSchemaId(schemaId);
    }

    KafkaEventReporter kafkaReporter = builder.build("localhost:0000", "topic");

    GobblinTrackingEvent event = new GobblinTrackingEvent(0l, "namespace", "name", Maps.<String, String>newHashMap());

    context.submitEvent(event);

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    byte[] nextMessage = pusher.messageIterator().next();
    DataInputStream is = new DataInputStream(new ByteArrayInputStream(nextMessage));
    Assert.assertEquals(is.readByte(), KafkaAvroSchemaRegistry.MAGIC_BYTE);
    byte[] readId = new byte[20];
    Assert.assertEquals(is.read(readId), 20);
    String readStringId = Hex.encodeHexString(readId);
    if (!isSchemaIdEnabled) {
      Assert.assertTrue(this.schemas.containsKey(readStringId));
      Schema readSchema = this.schemas.get(readStringId);
      Assert.assertFalse(readSchema.toString().contains("avro.java.string"));
    }
    Assert.assertEquals(readStringId, schemaId);
    is.close();
  }
}
