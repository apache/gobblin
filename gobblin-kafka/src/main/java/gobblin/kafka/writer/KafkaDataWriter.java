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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.writer.DataWriter;


/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This does not provide transactional / exactly-once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class KafkaDataWriter<D> implements DataWriter<D> {

  private final KafkaProducer<String, D> producer;
  private final Callback producerCallback;
  private final String topic;
  private final AtomicInteger recordsWritten;
  private final AtomicInteger bytesWritten;


  private static final String KEY_SERIALIZER_CONFIG = "key.serializer";
  private static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  private static final String VALUE_SERIALIZER_CONFIG = "value.serializer";
  private static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";


  private static Properties getProducerProperties(Properties props)
  {
    Properties producerProperties = stripPrefix(props, KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX);

    //Provide default properties if not set from above
    if (!producerProperties.containsKey(KEY_SERIALIZER_CONFIG)) {
      producerProperties.setProperty(KEY_SERIALIZER_CONFIG, DEFAULT_KEY_SERIALIZER);
    }
    if (!producerProperties.containsKey(VALUE_SERIALIZER_CONFIG)) {
      producerProperties.setProperty(VALUE_SERIALIZER_CONFIG, DEFAULT_VALUE_SERIALIZER);
    }
    return producerProperties;
  }


  public KafkaDataWriter(Properties props) {
    this(new KafkaProducer<>(getProducerProperties(props)), ConfigFactory.parseProperties(props));
  }

  public KafkaDataWriter(KafkaProducer producer, Config config)
  {
    recordsWritten = new AtomicInteger(0);
    bytesWritten = new AtomicInteger(-1);

    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    this.producer = producer;
    this.producerCallback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (null == exception)
        {
          recordsWritten.incrementAndGet();
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
    this.producer.send(new ProducerRecord<String, D>(topic, record), this.producerCallback);
  }

  @Override
  public void close()
      throws IOException {
    this.producer.close();
  }

  @Override
  public void commit()
      throws IOException {
  }

  @Override
  public void cleanup()
      throws IOException {
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
