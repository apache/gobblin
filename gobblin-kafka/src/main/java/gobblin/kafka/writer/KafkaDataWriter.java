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
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.util.ConfigUtils;
import gobblin.writer.DataWriter;

import static gobblin.kafka.writer.KafkaWriterConfigurationKeys.*;
/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This does not provide transactional / exactly-once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class KafkaDataWriter<D> implements DataWriter<D> {

  private static final long MILLIS_TO_NANOS = 1000 * 1000;

  private final KafkaProducer<String, D> producer;
  private final Callback producerCallback;
  private final String topic;
  private final long commitTimeoutInNanos;
  private final AtomicInteger bytesWritten;
  private final AtomicInteger recordsWritten;
  private long recordsProduced;
  private long recordsFailed;
  private final Lock lock;
  private final Condition stateChange;


  private static Properties getProducerProperties(Properties props)
  {
    Properties producerProperties = stripPrefix(props, KAFKA_PRODUCER_CONFIG_PREFIX);

    // Provide default properties if not set from above
    setDefaultIfUnset(producerProperties, KEY_SERIALIZER_CONFIG, DEFAULT_KEY_SERIALIZER);
    setDefaultIfUnset(producerProperties, VALUE_SERIALIZER_CONFIG, DEFAULT_VALUE_SERIALIZER);
    setDefaultIfUnset(producerProperties, CLIENT_ID_CONFIG, CLIENT_ID_DEFAULT);
    return producerProperties;
  }

  private static void setDefaultIfUnset(Properties props, String key, String value)
  {
    if (!props.containsKey(key)) {
      props.setProperty(key, value);
    }
  }

  public static KafkaProducer getKafkaProducer(Properties props)
  {
    Config config = ConfigFactory.parseProperties(props);
    String kafkaProducerClass = ConfigUtils.getString(config, KafkaWriterConfigurationKeys.KAFKA_WRITER_PRODUCER_CLASS, "");
    Properties producerProps = getProducerProperties(props);
    if (kafkaProducerClass.isEmpty())
    {
      return new KafkaProducer<>(producerProps);
    }
    else
    {
      try {
        Class<?> producerClass = (Class<?>) Class.forName(kafkaProducerClass);
        KafkaProducer producer = (KafkaProducer) ConstructorUtils.invokeConstructor(producerClass, producerProps);
        return producer;
      } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
        log.error("Failed to instantiate Kafka producer " + kafkaProducerClass + " as instance of KafkaProducer.class", e);
        throw Throwables.propagate(e);
      }
    }
  }

  public KafkaDataWriter(Properties props) {
    this(getKafkaProducer(props), ConfigFactory.parseProperties(props));
  }

  public KafkaDataWriter(KafkaProducer producer, Config config)
  {
    recordsProduced = 0;
    recordsWritten = new AtomicInteger(0);
    recordsFailed = 0;
    bytesWritten = new AtomicInteger(-1);
    lock = new ReentrantLock();
    stateChange = lock.newCondition();
    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    this.commitTimeoutInNanos = ConfigUtils.getLong(config, KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_CONFIG,
        KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_DEFAULT) * MILLIS_TO_NANOS;
    this.producer = producer;
    this.producerCallback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        lock.lock();
        try {
          if (null == exception) {
            recordsWritten.incrementAndGet();
          } else {
            log.warn("record failed to write", exception);
            recordsFailed++;
          }
          stateChange.signal();
        }
        finally {
          lock.unlock();
        }
      }
    };
  }

  private static Properties stripPrefix(Properties props, String prefix) {
    Properties strippedProps = new Properties();
    int prefixLength = prefix.length();
    for (String key: props.stringPropertyNames())
    {
      if (key.startsWith(prefix))
      {
        strippedProps.setProperty(key.substring(prefixLength), props.getProperty(key));
      }
    }
    return strippedProps;
  }

  @Override
  public void write(D record)
      throws IOException {
    log.debug("Write called");
    lock.lock();
    try
    {
      this.producer.send(new ProducerRecord<String, D>(topic, record), this.producerCallback);
      recordsProduced++;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.producer.close();
  }

  @Override
  public void commit()
      throws IOException {
    log.debug("Commit called, will wait for commitTimeout : " + commitTimeoutInNanos / MILLIS_TO_NANOS + "ms");
    long commitStartTime = System.nanoTime();
    lock.lock();
    try {
      while (((System.nanoTime() - commitStartTime) < commitTimeoutInNanos)  &&
      (recordsProduced != (recordsWritten.get() + recordsFailed)))
      {
        log.debug("Commit waiting... records produced: " + recordsProduced + " written: " + recordsWritten.get() + " failed: " + recordsFailed);
        try {
          stateChange.awaitNanos(commitTimeoutInNanos);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      log.debug("Commit done waiting");
      if (recordsProduced != (recordsWritten.get() + recordsFailed)) // timeout
      {
        String message = "Commit timeout: RecordsWritten = " + recordsWritten.get()
            + "; RecordsFailed = " + recordsFailed;
        log.debug(message);
        throw new IOException(message);
      }
      if (recordsFailed > 0)
      {
        String message = "Commit failed to write " + recordsFailed + " records.";
        log.debug(message);
        throw new IOException(message);
      }
      log.debug("Commit successful...");
    }
    finally{
      lock.unlock();
    }
  }

  @Override
  public void cleanup()
      throws IOException {
    log.debug("Cleanup called");

  }

  @Override
  public long recordsWritten() {
    return recordsWritten.get();
  }

  @Override
  public long bytesWritten() {
    return bytesWritten.get();
  }
}
