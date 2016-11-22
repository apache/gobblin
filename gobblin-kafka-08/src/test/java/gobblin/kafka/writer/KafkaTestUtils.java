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

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class KafkaTestUtils {

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

  static byte[] generateRandomBytes() {
    Random rng = new Random();
    int length = rng.nextInt(200);
    byte[] messageBytes = new byte[length];
    rng.nextBytes(messageBytes);
    return messageBytes;
  }


  static GenericRecord generateRandomAvroRecord() {

    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    String fieldName = "field1";
    Schema fieldSchema = Schema.create(Schema.Type.STRING);
    String docString = "doc";
    fields.add(new Schema.Field(fieldName, fieldSchema, docString, null));
    Schema schema = Schema.createRecord("name", docString, "test",false);
    schema.setFields(fields);

    GenericData.Record record = new GenericData.Record(schema);
    record.put("field1", "foobar");

    return record;

  }
}
