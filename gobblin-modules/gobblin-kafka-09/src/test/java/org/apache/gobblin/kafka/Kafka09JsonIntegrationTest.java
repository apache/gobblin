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

package org.apache.gobblin.kafka;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.kafka.writer.Kafka09JsonObjectWriterBuilder;
import org.apache.gobblin.runtime.util.MultiWorkUnitUnpackingIterator;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.kafka.Kafka09JsonSource;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.Destination;

import static org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX;
import static org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys.KAFKA_TOPIC;


/**
 * An integration test for {@link Kafka09JsonSource} and {@link Kafka09JsonObjectWriterBuilder}. The test writes
 * a json object to kafka with the writer and extracts it with the source
 */
@Slf4j
public class Kafka09JsonIntegrationTest {
  private final Gson gson;
  private final KafkaTestBase kafkaTestHelper;

  public Kafka09JsonIntegrationTest()
      throws InterruptedException, RuntimeException {
    kafkaTestHelper = new KafkaTestBase();
    gson = new Gson();
  }

  @BeforeSuite
  public void beforeSuite() {
    log.info("Process id = " + ManagementFactory.getRuntimeMXBean().getName());
    kafkaTestHelper.startServers();
  }

  @AfterSuite
  public void afterSuite()
      throws IOException {
    try {
      kafkaTestHelper.stopClients();
    } finally {
      kafkaTestHelper.stopServers();
    }
  }

  private SourceState createSourceState(String topic) {
    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + kafkaTestHelper.getKafkaServerPort());
    state.setProp(KafkaSource.TOPIC_WHITELIST, topic);
    state.setProp(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS,
        Kafka09ConsumerClient.Factory.class.getName());
    state.setProp(KafkaSource.BOOTSTRAP_WITH_OFFSET, "earliest");
    return state;
  }

  @Test
  public void testHappyPath()
      throws IOException, DataRecordException {
    String topic = "testKafka09JsonSource";
    kafkaTestHelper.provisionTopic(topic);
    SourceState state = createSourceState(topic);

    // Produce a record
    state.setProp(KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers",
        "localhost:" + kafkaTestHelper.getKafkaServerPort());
    state.setProp(KAFKA_TOPIC, topic);
    Destination destination = Destination.of(Destination.DestinationType.KAFKA, state);
    Kafka09JsonObjectWriterBuilder writerBuilder = new Kafka09JsonObjectWriterBuilder();
    writerBuilder.writeTo(destination);
    DataWriter<JsonObject> writer = writerBuilder.build();

    final String json = "{\"number\":27}";
    JsonObject record = gson.fromJson(json, JsonObject.class);
    writer.write(record);
    writer.flush();
    writer.close();

    Kafka09JsonSource source = new Kafka09JsonSource();
    List<WorkUnit> workUnitList = source.getWorkunits(state);
    // Test the right value serializer is set
    Assert.assertEquals(state.getProp(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY),
        Kafka09JsonSource.KafkaGsonDeserializer.class.getName());

    // Test there is only one non-empty work unit
    MultiWorkUnitUnpackingIterator iterator = new MultiWorkUnitUnpackingIterator(workUnitList.iterator());
    Assert.assertTrue(iterator.hasNext());
    WorkUnit workUnit = iterator.next();
    Assert.assertEquals(workUnit.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY), topic);
    Assert.assertFalse(iterator.hasNext());

    // Test extractor
    WorkUnitState workUnitState = new WorkUnitState(workUnit, state);

    final String jsonSchema =
        "[{\"columnName\":\"number\",\"comment\":\"\",\"isNullable\":\"false\",\"dataType\":{\"type\":\"int\"}}]";
    workUnitState.setProp("source.kafka.json.schema", jsonSchema);

    Extractor<JsonArray, JsonObject> extractor = source.getExtractor(workUnitState);
    Assert.assertEquals(extractor.getSchema().toString(), jsonSchema);
    Assert.assertEquals(extractor.readRecord(null).toString(), json);
  }
}
