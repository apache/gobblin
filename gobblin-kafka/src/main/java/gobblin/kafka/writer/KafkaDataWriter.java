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

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.codahale.metrics.Meter;
import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.instrumented.writer.InstrumentedDataWriter;
import gobblin.util.ConfigUtils;

import static gobblin.kafka.writer.KafkaWriterConfigurationKeys.*;
/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This does not provide transactional / exactly-once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class KafkaDataWriter<D> extends InstrumentedDataWriter<D> {

  private static final long MILLIS_TO_NANOS = 1000 * 1000;

  private final Producer<String, D> producer;
  private final Callback producerCallback;
  private final String topic;
  private final long commitTimeoutInNanos;
  private final long commitStepWaitTimeMillis;
  private final double failureAllowance;
  private final AtomicInteger bytesWritten;
  private final Meter recordsWritten;
  private final Meter recordsFailed;
  private final Meter recordsProduced;


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

  public static Producer getKafkaProducer(Properties props)
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
        Producer producer = (Producer) ConstructorUtils.invokeConstructor(producerClass, producerProps);
        return producer;
      } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
        log.error("Failed to instantiate Kafka producer " + kafkaProducerClass + " as instance of Producer.class", e);
        throw Throwables.propagate(e);
      }
    }
  }

  public KafkaDataWriter(Properties props) {
    this(getKafkaProducer(props), ConfigFactory.parseProperties(props));
  }

  public KafkaDataWriter(Producer producer, Config config)
  {
    super(ConfigUtils.configToState(config));
    recordsProduced = getMetricContext().meter(KafkaWriterMetricNames.RECORDS_PRODUCED_METER);
    recordsWritten = getMetricContext().meter(KafkaWriterMetricNames.RECORDS_SUCCESS_METER);
    recordsFailed = getMetricContext().meter(KafkaWriterMetricNames.RECORDS_FAILED_METER);
    bytesWritten = new AtomicInteger(-1);
    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    this.commitTimeoutInNanos = ConfigUtils.getLong(config, KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_CONFIG,
        KafkaWriterConfigurationKeys.COMMIT_TIMEOUT_MILLIS_DEFAULT) * MILLIS_TO_NANOS;
    this.commitStepWaitTimeMillis = ConfigUtils.getLong(config, KafkaWriterConfigurationKeys.COMMIT_STEP_WAIT_TIME_CONFIG,
        KafkaWriterConfigurationKeys.COMMIT_STEP_WAIT_TIME_DEFAULT);
    this.failureAllowance = ConfigUtils.getDouble(config, KafkaWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_CONFIG,
        KafkaWriterConfigurationKeys.FAILURE_ALLOWANCE_PCT_DEFAULT) / 100.0;
    this.producer = producer;
    this.producerCallback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (null == exception) {
            recordsWritten.mark();
          } else {
            log.debug("record failed to write", exception);
            recordsFailed.mark();
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
  public void writeImpl(D record)
      throws IOException {
    log.debug("Write called");
    this.producer.send(new ProducerRecord<String, D>(topic, record), this.producerCallback);
    recordsProduced.mark();
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    try {
      this.producer.close();
    }
    finally {
      super.close();
    }
  }

  @Override
  public void commit()
      throws IOException {
    log.debug("Commit called, will wait for commitTimeout : " + commitTimeoutInNanos / MILLIS_TO_NANOS + "ms");
    long commitStartTime = System.nanoTime();
    while (((System.nanoTime() - commitStartTime) < commitTimeoutInNanos)  &&
        (recordsProduced.getCount() != (recordsWritten.getCount() + recordsFailed.getCount())))
    {
      log.debug("Commit waiting... records produced: " + recordsProduced.getCount() + " written: "
          + recordsWritten.getCount() + " failed: " + recordsFailed.getCount());
      try {
        Thread.sleep(commitStepWaitTimeMillis);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for commit to complete", e);
      }
    }
    log.debug("Commit done waiting");
    long recordsProducedFinal = recordsProduced.getCount();
    long recordsWrittenFinal = recordsWritten.getCount();
    long recordsFailedFinal = recordsFailed.getCount();
    long unacknowledgedWrites  = recordsProducedFinal - recordsWrittenFinal - recordsFailedFinal;
    long totalFailures = unacknowledgedWrites + recordsFailedFinal;
    if (unacknowledgedWrites > 0) // timeout
    {
      log.warn("Timeout waiting for all writes to be acknowledged. Missing " + unacknowledgedWrites
          + " responses out of " + recordsProducedFinal);
    }
    if (totalFailures > 0 && recordsProducedFinal > 0)
    {
      String message = "Commit failed to write " + totalFailures
          + " records (" + recordsFailedFinal + " failed, " + unacknowledgedWrites + " unacknowledged) out of "
          + recordsProducedFinal + " produced.";
      double failureRatio = (double)totalFailures / (double)recordsProducedFinal;
      if (failureRatio > failureAllowance)
      {
        message += "\nAborting because this is greater than the failureAllowance percentage: " + failureAllowance*100.0;
        log.error(message);
        throw new IOException(message);
      }
      else
      {
        message += "\nCommitting because failureRatio percentage: " + (failureRatio * 100.0) +
            " is less than the failureAllowance percentage: " + (failureAllowance * 100.0);
        log.warn(message);
      }
    }
    log.info("Successfully committed " + recordsWrittenFinal + " records.");
  }

  @Override
  public void cleanup()
      throws IOException {
    log.debug("Cleanup called");

  }

  @Override
  public long recordsWritten() {
    return recordsWritten.getCount();
  }

  @Override
  public long bytesWritten() {
    return bytesWritten.get();
  }
}
