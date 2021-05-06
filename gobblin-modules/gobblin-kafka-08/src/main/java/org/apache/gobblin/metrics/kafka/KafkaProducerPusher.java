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

package org.apache.gobblin.metrics.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gobblin.KafkaCommonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.base.Optional;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;


/**
 * Establishes a connection to a Kafka cluster and push byte messages to a specified topic.
 */
@Slf4j
public class KafkaProducerPusher implements Pusher<byte[]> {
  private static final long DEFAULT_MAX_NUM_FUTURES_TO_BUFFER = 1000L;
  //Low watermark for the size of the futures queue, to trigger flushing of messages.
  private static final String MAX_NUM_FUTURES_TO_BUFFER_KEY = "numFuturesToBuffer";

  private final String topic;
  private final KafkaProducer<String, byte[]> producer;
  private final Closer closer;
  //Queue to keep track of the futures returned by the Kafka asynchronous send() call. The futures queue is used
  // to mimic the functionality of flush() call (available in Kafka 09 and later). Currently, there are no
  // capacity limits on the size of the futures queue. In general, if queue capacity is enforced, a safe lower bound for queue
  // capacity is MAX_NUM_FUTURES_TO_BUFFER + (numThreads * maxNumMessagesPerInterval), where numThreads equals the number of
  // threads sharing the producer instance and maxNumMessagesPerInterval is the estimated maximum number of messages
  // emitted by a thread per reporting interval.
  private final Queue<Future<RecordMetadata>> futures = new LinkedBlockingDeque<>();
  private long numFuturesToBuffer = 1000L;

  public KafkaProducerPusher(String brokers, String topic, Optional<Config> kafkaConfig) {
    this.closer = Closer.create();

    this.topic = topic;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);

    // add the kafka scoped config. if any of the above are specified then they are overridden
    if (kafkaConfig.isPresent()) {
      props.putAll(ConfigUtils.configToProperties(kafkaConfig.get()));
      this.numFuturesToBuffer = ConfigUtils.getLong(kafkaConfig.get(), MAX_NUM_FUTURES_TO_BUFFER_KEY, DEFAULT_MAX_NUM_FUTURES_TO_BUFFER);
    }

    this.producer = createProducer(props);
  }

  public KafkaProducerPusher(String brokers, String topic) {
    this(brokers, topic, Optional.absent());
  }

  /**
   * Push all byte array messages to the Kafka topic.
   * @param messages List of byte array messages to push to Kakfa.
   */
  public void pushMessages(List<byte[]> messages) {
    for (byte[] message: messages) {
      this.futures.offer(producer.send(new ProducerRecord<>(topic, message), (recordMetadata, e) -> {
        if (e != null) {
          log.error("Failed to send message to topic {} due to exception: ", topic, e);
        }
      }));
    }

    //Once the low watermark of numFuturesToBuffer is hit, start flushing messages from the futures
    // buffer. In order to avoid blocking on newest messages added to futures queue, we only invoke future.get() on
    // the oldest messages in the futures buffer. The number of messages to flush is same as the number of messages added
    // in the current call. Note this does not completely avoid calling future.get() on the newer messages e.g. when
    // multiple threads enter the if{} block concurrently, and invoke flush().
    if (this.futures.size() >= this.numFuturesToBuffer) {
      flush(messages.size());
    }
  }

  /**
   * Flush any records that may be present in the producer buffer upto a maximum of <code>numRecordsToFlush</code>.
   * This method is needed since Kafka 0.8 producer does not have a flush() API. In the absence of the flush()
   * implementation, records which are present in the buffer but not in-flight may not be delivered at all when close()
   * is called, leading to data loss.
   * @param numRecordsToFlush
   */
  private void flush(long numRecordsToFlush) {
    log.debug("Flushing records from producer buffer");
    Future future;
    long numRecordsFlushed = 0L;
    while (((future = futures.poll()) != null) && (numRecordsFlushed++ < numRecordsToFlush)) {
      try {
        future.get();
      } catch (Exception e) {
        log.error("Exception encountered when flushing record", e);
      }
    }
    log.debug("Flushed {} records from producer buffer", numRecordsFlushed);
  }

  @Override
  public void close()
      throws IOException {
    log.info("Flushing records before close");
    //Call flush() before invoking close() to ensure any buffered messages are immediately sent. This is required
    //since close() only guarantees delivery of in-flight messages. Set a timeout to prevent GOBBLIN-1432 issue.
    //This issue shouldn't exits in later version, as native flush function has a timeout setting offset.flush.timeout.ms
    try {
      KafkaCommonUtil.runWithTimeout(
          () -> flush(Long.MAX_VALUE), KafkaCommonUtil.KAFKA_FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      log.warn("Flush records before close was interrupted! Reached {} seconds timeout!",
          KafkaCommonUtil.KAFKA_FLUSH_TIMEOUT_SECONDS);
    } catch (Exception e) {
      log.error("Exception encountered when flushing record before close", e);
    }
    this.closer.close();
  }

  /**
   * Create the Kafka producer.
   */
  protected KafkaProducer<String, byte[]> createProducer(Properties props) {
    return this.closer.register(new KafkaProducer<String, byte[]>(props));
  }
}
