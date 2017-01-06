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
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.util.ConfigUtils;

import static gobblin.kafka.writer.KafkaWriterConfigurationKeys.*;
/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This does not provide transactional / exactly-once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class Kafka09DataWriter<D> implements AsyncDataWriter<D> {

  private final Producer<String, D> producer;
  private Callback producerCallback;
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
    this.producerCallback = null;
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.producer.close();
  }

  @Override
  public void cleanup()
      throws IOException {
    log.debug("Cleanup called");

  }

  @Override
  public long bytesWritten() {
    // Unfortunately since we are not in control of serialization,
    // we don't really know how many bytes we've written
    return -1;
  }

  @Override
  public void setDefaultCallback(final WriteCallback callback) {
    this.producerCallback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null)
        {
          callback.onFailure(exception);
        }
        else
        {
          callback.onSuccess();
        }
      }
    };
  }


  @Override
  public void asyncWrite(D record) {
    this.producer.send(new ProducerRecord<String, D>(topic, record), this.producerCallback);
  }
}
