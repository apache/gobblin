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
package gobblin.kafka.source.extractor.extract.kafka;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.sun.tools.javac.util.Assert;

import lombok.extern.slf4j.Slf4j;

import gobblin.source.extractor.DataRecordException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.kafka.KafkaTestBase;
import gobblin.configuration.State;
import gobblin.configuration.SourceState;
import gobblin.source.extractor.extract.kafka.KafkaSimpleStreamingExtractor;
import gobblin.source.extractor.extract.kafka.KafkaSimpleStreamingSource;
import gobblin.source.extractor.RecordEnvelope;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.extractor.extract.LongWatermark;


import static org.mockito.Mockito.verify;


/**
 * Simple unit tests for the streaming kafka producer.Covers very simple scenarios
 */
@Slf4j
public class KafkaSimpleStreamingTest {
  private final KafkaTestBase _kafkaTestHelper;
  public KafkaSimpleStreamingTest()
      throws InterruptedException, RuntimeException {
    _kafkaTestHelper = new KafkaTestBase();
  }

  @BeforeSuite
  public void beforeSuite() {
    log.info("Process id = " + ManagementFactory.getRuntimeMXBean().getName());

    _kafkaTestHelper.startServers();
  }

  @AfterSuite
  public void afterSuite()
      throws IOException {
    try {
      _kafkaTestHelper.stopClients();
    }
    finally {
      _kafkaTestHelper.stopServers();
    }
  }

  /**
   * Tests that the source creates workUnits appropriately. Sets up a topic with a single partition and checks that a
   * single workUnit is returned with the right parameters sets
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testSource()
      throws IOException, InterruptedException {
    String topic = "testSimpleStreamingSource";
    _kafkaTestHelper.provisionTopic(topic);
    SourceState ss = new SourceState();
    ss.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    ss.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaSimpleStreamingSource<String, byte[]> simpleSource = new KafkaSimpleStreamingSource<String, byte[]>();
    List<WorkUnit> lWu = simpleSource.getWorkunits(ss);
    // Check we have a single WorkUnit with the right properties setup.
    Assert.check(lWu.size() == 1);
    WorkUnit wU = lWu.get(0);
    Assert.check(KafkaSimpleStreamingSource.getTopicNameFromState(wU).equals(topic));
    Assert.check(KafkaSimpleStreamingSource.getPartitionIdFromState(wU) == 0);
  }

  /**
   * testExtractor checks that the extractor code does the right thing. First it creates a topic, and sets up a source to point
   * to it. workUnits are generated from the source (only a single wU should be returned). Then it writes a record to this topic
   * and reads back from the extractor to verify the right record is returned. A second record is then written and read back
   * through the extractor to verify poll works as expected. Finally we test the commit api by forcing a commit and then starting
   * a new extractor to ensure we fetch data from after the commit. The commit is also verified in Kafka directly
   * @throws IOException
   * @throws InterruptedException
   * @throws DataRecordException
   */
  @Test(timeOut = 10000)
  public void testExtractor()
      throws IOException, InterruptedException, DataRecordException {
    final String topic = "testSimpleStreamingExtractor";
    _kafkaTestHelper.provisionTopic(topic);

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    Producer<String, byte[]> producer = new KafkaProducer<>(props);

    final byte [] record_1 = {0, 1, 3};
    final byte [] record_2 = {2, 4, 6};
    final byte [] record_3 = {5, 7, 9};

    // Write a sample record to the topic
    producer.send(new ProducerRecord<String, byte[]>(topic, topic, record_1));
    producer.flush();

    SourceState ss = new SourceState();
    ss.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    ss.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaSimpleStreamingSource<String, byte[]> simpleSource = new KafkaSimpleStreamingSource<String, byte[]>();

    List<WorkUnit> lWu = simpleSource.getWorkunits(ss);
    WorkUnit wU = lWu.get(0);
    WorkUnitState wSU = new WorkUnitState(wU, new State());
    wSU.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());

    wSU.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    wSU.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);
    wSU.setProp(KafkaSimpleStreamingSource.TOPIC_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
    wSU.setProp(KafkaSimpleStreamingSource.TOPIC_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    // Create an extractor
    KafkaSimpleStreamingExtractor<String, byte[]> kSSE = new KafkaSimpleStreamingExtractor<String, byte[]>(wSU);
    TopicPartition tP = new TopicPartition(topic, 0);
    KafkaSimpleStreamingExtractor.KafkaWatermark kwm =
        new KafkaSimpleStreamingExtractor.KafkaWatermark(tP, new LongWatermark(0));
    byte [] reuse = new byte[1];
    RecordEnvelope<byte[]> oldRecord = new RecordEnvelope<>(reuse, kwm);

    // read and verify the record matches we just wrote
    RecordEnvelope<byte[]> record = kSSE.readRecord(oldRecord);
    Assert.check(Arrays.equals(record.getRecord(), record_1));

    // write a second record.
    producer.send(new ProducerRecord<String, byte[]>(topic, topic, record_2));
    producer.flush();

    // read the second record using same extractor to verify it matches whats expected
    record = kSSE.readRecord(oldRecord);
    Assert.check(Arrays.equals(record.getRecord(), record_2));

    // Call the commit watermark
    kSSE.commitWatermarks(Collections.singletonList(record.getWatermark()));

    Consumer<String, byte[]> kC = KafkaSimpleStreamingSource.getKafkaConsumer(wSU);
    OffsetAndMetadata oM = kC.committed(tP);


    // verify committed watermark matches whats expected.
    Assert.check(record.getWatermark() instanceof KafkaSimpleStreamingExtractor.KafkaWatermark);
    KafkaSimpleStreamingExtractor.KafkaWatermark kWM = (KafkaSimpleStreamingExtractor.KafkaWatermark)record.getWatermark();
    Assert.check(oM.offset() == kWM.getLwm().getValue()+1);

    // write a third record.
    producer.send(new ProducerRecord<String, byte[]>(topic, topic, record_3));
    producer.flush();

    // recreate extractor to force a seek.
    kSSE = new KafkaSimpleStreamingExtractor(wSU);

    record = kSSE.readRecord(oldRecord);

    // check it matches the data written
    Assert.check(Arrays.equals(record.getRecord(), record_3));
  }

  /**
   * testThreadedExtractor verifies its safe to call close from a different thread when the original thread is stuck in poll
   * We create a topic and then wait for the extractor to return a record (which it never does) in a side thread. The
   * original thread calls close on the extractor and verifies the waiting thread gets an expected exception and exits
   * as expected.
   */
  @Test(timeOut = 10000)
  public void testThreadedExtractor() {
    final String topic = "testThreadedExtractor";
    _kafkaTestHelper.provisionTopic(topic);

    SourceState ss = new SourceState();
    ss.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    ss.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
    ss.setProp(KafkaSimpleStreamingSource.TOPIC_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaSimpleStreamingSource<String, byte[]> simpleSource = new KafkaSimpleStreamingSource<String, byte[]>();

    List<WorkUnit> lWu = simpleSource.getWorkunits(ss);
    WorkUnit wU = lWu.get(0);
    WorkUnitState wSU = new WorkUnitState(wU, new State());
    wSU.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + _kafkaTestHelper.getKafkaServerPort());

    wSU.setProp(KafkaSimpleStreamingSource.TOPIC_WHITELIST, topic);
    wSU.setProp(ConfigurationKeys.JOB_NAME_KEY, topic);
    wSU.setProp(KafkaSimpleStreamingSource.TOPIC_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
    wSU.setProp(KafkaSimpleStreamingSource.TOPIC_VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    // Create an extractor
    final KafkaSimpleStreamingExtractor<String, byte[]> kSSE = new KafkaSimpleStreamingExtractor<String, byte[]>(wSU);

    Thread waitingThread = new Thread () {
      public void run () {
        TopicPartition tP = new TopicPartition(topic, 0);
        KafkaSimpleStreamingExtractor.KafkaWatermark kwm =
            new KafkaSimpleStreamingExtractor.KafkaWatermark(tP, new LongWatermark(0));
        byte[] reuse = new byte[1];
        RecordEnvelope<byte[]> oldRecord = new RecordEnvelope<>(reuse, kwm);
        try {
          RecordEnvelope<byte[]> record = kSSE.readRecord(oldRecord);
        } catch (Exception e) {
          Assert.check((e instanceof WakeupException) || (e instanceof ClosedChannelException));
        }
      }
    };
    waitingThread.start();
    try {
      kSSE.close();
      waitingThread.join();
    } catch (Exception e) {
      // should never come here
      throw new Error(e);
    }
  }

}
