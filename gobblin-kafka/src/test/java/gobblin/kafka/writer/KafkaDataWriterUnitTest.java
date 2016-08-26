/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka.writer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.kafka.ErrorManager;
import gobblin.kafka.FlakyKafkaProducer;


/**
 * Tests that don't need Kafka server to be running
 * */

@Slf4j
public class KafkaDataWriterUnitTest {

  class TimingResult {
    private final boolean isSync;
    private final long timeValueMillis;

    TimingResult(boolean isSync, long timeValueMillis)
    {
      this.isSync = isSync;
      this.timeValueMillis = timeValueMillis;
    }

  }

  interface TimingType {
    public long nextTimeMillis();
  }


  class ConstantTimingType implements TimingType {
    private long timeDurationMillis;
    ConstantTimingType(long timeDurationMillis)
    {
      this.timeDurationMillis = timeDurationMillis;
    }

    public long nextTimeMillis() {
      return this.timeDurationMillis;
    }
  }

  class NthTimingType implements TimingType {
    private final int n;
    private final long defaultTimeMillis;
    private final long nthTimeMillis;
    private int currentNum;

    NthTimingType(int n, long defaultTimeMillis, long nthTimeMillis)
    {
      this.n = n;
      this.defaultTimeMillis = defaultTimeMillis;
      this.nthTimeMillis = nthTimeMillis;
      this.currentNum = 0;
    }

    @Override
    public long nextTimeMillis() {
      currentNum++;
      if (currentNum % n == 0)
      {
        return nthTimeMillis;
      }
      else
      {
        return defaultTimeMillis;
      }
    }
  }

  class TimingManager {

    private final boolean isSync;
    private final TimingType timingType;

    public TimingManager(boolean isSync, TimingType timingType)
    {
      this.isSync = isSync;
      this.timingType = timingType;
    }

    TimingResult nextTime()
    {
      return new TimingResult(isSync, timingType.nextTimeMillis());
    }
  }

  class FakeTimedKafkaProducer<K,V> extends KafkaProducer<K,V>
  {

    TimingManager timingManager;

    public FakeTimedKafkaProducer(TimingManager timingManager, Properties properties) {
      super(properties);
      this.timingManager = timingManager;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, final Callback producerCallback)
    {
      final TimingResult result = this.timingManager.nextTime();
      log.debug("sync: " + result.isSync + " time : " + result.timeValueMillis);
      TopicPartition topicPartition = new TopicPartition(record.topic(), 0);
      long baseOffset = -1L;
      long relativeOffset = 1L;
      final RecordMetadata recordMetadata = new RecordMetadata(topicPartition, baseOffset, relativeOffset);
      if (result.isSync)
      {
        try {
          Thread.sleep(result.timeValueMillis);
        } catch (InterruptedException e) {
        }
        producerCallback.onCompletion(recordMetadata, null);
      }
      else
      {
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              log.debug("Sleeping for ms: " + result.timeValueMillis);
              Thread.sleep(result.timeValueMillis);
            } catch (InterruptedException e) {
            }
            producerCallback.onCompletion(recordMetadata, null);
          }
        });
        t.start();
      }

      return null;
    }


  }

  @Test
  public void testSlowKafka() throws Exception {
    // Every call incurs 1s latency, commit timeout is 40s
    testKafkaWrites(new ConstantTimingType(1000), 40000, 0, true);
    // Every call incurs 10s latency, commit timeout is 4s
    testKafkaWrites(new ConstantTimingType(10000), 4000, 0, false);
    // Every 7th call incurs 10s latency, every other call incurs 1s latency
    testKafkaWrites(new NthTimingType(7, 1000, 10000), 4000, 0, false);
    // Every 7th call incurs 10s latency, every other call incurs 1s latency, failures allowed < 11%
    testKafkaWrites(new NthTimingType(7, 1000, 10000), 4000, 11, true);

  }

  private void testKafkaWrites(TimingType timingType, long commitTimeout, double failurePercentage, boolean success)
  {
    TimingManager timingManager = new TimingManager(false, timingType); // 1s latency per record
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, "Test");
    props.setProperty(KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_CONFIG, "" + commitTimeout);
    props.setProperty(KafkaWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_CONFIG, "" + failurePercentage);
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("key.serializer", KafkaWriterConfigurationKeys.DEFAULT_KEY_SERIALIZER);
    props.setProperty("value.serializer", KafkaWriterConfigurationKeys.DEFAULT_VALUE_SERIALIZER);
    Config config = ConfigFactory.parseProperties(props);

    KafkaProducer<String, byte[]> slowKafkaProducer = new FakeTimedKafkaProducer<>(timingManager, props);
    KafkaDataWriter<byte[]> kafkaWriter = new KafkaDataWriter<byte[]>(slowKafkaProducer, config);

    try {
      for (int i = 0; i < 10; i++) {
        kafkaWriter.write(KafkaTestUtils.generateRandomBytes());
      }
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw any Exception");
    }

    try {
      kafkaWriter.commit();
      if (!success) {
        Assert.fail("Commit should not succeed");
      }
    }
    catch (IOException e)
    {
      if (success)
      {
        Assert.fail("Commit should not throw IOException");
      }
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw any exception other than IOException");
    }
    try {
      kafkaWriter.close();
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw any exception on close");
    }


  }


  @Test
  public void testMinimalConfig()
  {
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, "FakeTopic");
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", "localhost:9092");

    try {
      KafkaDataWriter<GenericRecord> kafkaWriter = new KafkaDataWriter<>(props);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw exception", e);
    }

  }


  @Test
  public void testCompleteFailureMode() throws Exception {
    String topic = "testFailureMode";
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);

    Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", "localhost:9092");
    producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty(ErrorManager.ERROR_TYPE_CONFIGURATION_KEY, "all");
    FlakyKafkaProducer flakyKafkaProducer = new FlakyKafkaProducer(producerProps);

    KafkaDataWriter<byte[]> kafkaWriter = new KafkaDataWriter<>(flakyKafkaProducer, ConfigFactory.parseProperties(props));

    byte[] messageBytes = KafkaTestUtils.generateRandomBytes();
    kafkaWriter.write(messageBytes);

    try {
      kafkaWriter.commit();
    }
    catch (IOException e)
    {
      // ok for commit to throw exception
    }
    finally
    {
      kafkaWriter.close();
    }

    Assert.assertEquals(kafkaWriter.recordsWritten(), 0);

  }

}
