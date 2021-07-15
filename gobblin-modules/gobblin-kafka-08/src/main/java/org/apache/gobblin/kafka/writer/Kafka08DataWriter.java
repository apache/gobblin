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

package org.apache.gobblin.kafka.writer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationException;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.gobblin.writer.WriteResponseFuture;
import org.apache.gobblin.writer.WriteResponseMapper;


/**
 * Implementation of a Kafka writer that wraps a 0.8 {@link KafkaProducer}.
 * This does not provide transactional / exactly-once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class Kafka08DataWriter<K,V> implements KafkaDataWriter<K, V> {

  public static final WriteResponseMapper<RecordMetadata> WRITE_RESPONSE_WRAPPER =
      new WriteResponseMapper<RecordMetadata>() {

        @Override
        public WriteResponse wrap(final RecordMetadata recordMetadata) {
          return new WriteResponse<RecordMetadata>() {
            @Override
            public RecordMetadata getRawResponse() {
              return recordMetadata;
            }

            @Override
            public String getStringResponse() {
              return recordMetadata.toString();
            }

            @Override
            public long bytesWritten() {
              // Don't know how many bytes were written
              return -1;
            }
          };
        }
      };

  private final Producer<K, V> producer;
  private final String topic;
  private final KafkaWriterCommonConfig commonConfig;

  public static Producer getKafkaProducer(Properties props)
  {
    Object producerObject = KafkaWriterHelper.getKafkaProducer(props);
    try
    {
      Producer producer = (Producer) producerObject;
      return producer;
    } catch (ClassCastException e) {
      log.error("Failed to instantiate Kafka producer " + producerObject.getClass().getName() + " as instance of Producer.class", e);
      throw Throwables.propagate(e);
    }
  }

  public Kafka08DataWriter(Properties props)
      throws ConfigurationException {
    this(getKafkaProducer(props), ConfigFactory.parseProperties(props));
  }

  public Kafka08DataWriter(Producer producer, Config config)
      throws ConfigurationException {
    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    this.producer = producer;
    this.commonConfig = new KafkaWriterCommonConfig(config);
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.producer.close();
  }



  @Override
  public Future<WriteResponse> write(final V record, final WriteCallback callback) {
    try {
      Pair<K, V> kvPair = KafkaWriterHelper.getKeyValuePair(record, commonConfig);
      return write(kvPair, callback);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to generate write request", e);
    }
  }

  public Future<WriteResponse> write(Pair<K, V> keyValuePair, final WriteCallback callback) {
    try {
      return new WriteResponseFuture<>(this.producer
          .send(new ProducerRecord<>(topic, keyValuePair.getKey(), keyValuePair.getValue()), new Callback() {
            @Override
            public void onCompletion(final RecordMetadata metadata, Exception exception) {
              if (exception != null) {
                callback.onFailure(exception);
              } else {
                callback.onSuccess(WRITE_RESPONSE_WRAPPER.wrap(metadata));
              }
            }
          }), WRITE_RESPONSE_WRAPPER);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a Kafka write request", e);
    }
  }

  @Override
  public void flush()
      throws IOException {
    // Do nothing, 0.8 kafka producer doesn't support flush.
  }
}
