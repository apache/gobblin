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

package gobblin.kafka.writer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.writer.AsyncDataWriter;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;
import gobblin.writer.WriteResponseFuture;
import gobblin.writer.WriteResponseMapper;


/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This provides at-least once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class Kafka09DataWriter<D> implements AsyncDataWriter<D> {

  private static final WriteResponseMapper<RecordMetadata> WRITE_RESPONSE_WRAPPER =
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
              return -1;
            }
          };
        }
      };

  private final Producer<String, D> producer;
  private final String topic;

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

  public Kafka09DataWriter(Properties props) {
    this(getKafkaProducer(props), ConfigFactory.parseProperties(props));
  }

  public Kafka09DataWriter(Producer producer, Config config)
  {
    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    this.producer = producer;
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.producer.close();
  }


  @Override
  public Future<WriteResponse> write(final D record, final WriteCallback callback) {
    return new WriteResponseFuture<>(this.producer.send(new ProducerRecord<String, D>(topic, record), new Callback() {
      @Override
      public void onCompletion(final RecordMetadata metadata, Exception exception) {
        if (exception != null) {
          callback.onFailure(exception);
        } else {
          callback.onSuccess(WRITE_RESPONSE_WRAPPER.wrap(metadata));
        }
      }
    }), WRITE_RESPONSE_WRAPPER);
  }

  @Override
  public void flush()
      throws IOException {
    this.producer.flush();
  }
}
