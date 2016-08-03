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

package gobblin.runtime.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import gobblin.util.ConfigUtils;
import gobblin.util.ExecutorsUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A high level consumer for Kafka topics. Subclasses should implement {@link HighLevelConsumer#processMessage(MessageAndMetadata)}
 *
 * Note each thread will block for each message until {@link #processMessage(MessageAndMetadata)} returns, so for high
 * volume topics with few partitions {@link #processMessage(MessageAndMetadata)} must be fast or itself spawn more
 * threads.
 *
 * If threads > partitions in topic, extra threads will be idle.
 *
 * @param <K> type of the key.
 * @param <V> type of the value.
 */
@Slf4j
public abstract class HighLevelConsumer<K, V> extends AbstractIdleService {

  public static final String GROUP_ID_KEY = "group.id";
  // NOTE: changing this will break stored offsets
  private static final String DEFAULT_GROUP_ID = "KafkaJobSpecMonitor";

  private final String topic;
  private final ConsumerConnector consumer;
  private final int numThreads;
  private ExecutorService executor;

  public HighLevelConsumer(String topic, Config config, int numThreads) {
    this.consumer = createConsumerConnector(config);
    this.topic = topic;
    this.numThreads = numThreads;
  }

  /**
   * Called every time a message is read from the stream. Implementation must be thread-safe if {@link #numThreads} is
   * set larger than 1.
   */
  protected abstract void processMessage(MessageAndMetadata<K, V> message);

  @Override
  protected void startUp() {

    List<KafkaStream<byte[], byte[]>> streams = createStreams();
    this.executor = Executors.newFixedThreadPool(this.numThreads);

    // now create an object to consume the messages
    //
    int threadNumber = 0;
    for (final KafkaStream stream : streams) {
      this.executor.execute(new MonitorConsumer(stream));
      threadNumber++;
    }
  }

  protected ConsumerConnector createConsumerConnector(Config config) {

    Properties props = ConfigUtils.configToProperties(config);

    if (!props.containsKey(GROUP_ID_KEY)) {
      props.setProperty(GROUP_ID_KEY, DEFAULT_GROUP_ID);
    }
    ConsumerConfig consumerConfig = new ConsumerConfig(props);

    return Consumer.createJavaConsumerConnector(consumerConfig);
  }

  protected List<KafkaStream<byte[], byte[]>> createStreams() {
    Map<String, Integer> topicCountMap = Maps.newHashMap();
    topicCountMap.put(this.topic, this.numThreads);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = this.consumer.createMessageStreams(topicCountMap);
    return consumerMap.get(this.topic);
  }

  @Override
  public void shutDown() {
    if (this.consumer != null) {
      this.consumer.shutdown();
    }
    if (this.executor != null) {
      ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(log), 5000, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * A monitor for a Kafka stream.
   */
  @AllArgsConstructor
  public class MonitorConsumer implements Runnable {
    private final KafkaStream stream;

    public void run() {
      ConsumerIterator<K, V> it = this.stream.iterator();
      while (it.hasNext()) {
        MessageAndMetadata<K, V> message = it.next();
        HighLevelConsumer.this.processMessage(message);
      }
    }
  }

}
