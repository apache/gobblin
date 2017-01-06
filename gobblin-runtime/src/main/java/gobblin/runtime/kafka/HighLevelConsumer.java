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

package gobblin.runtime.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.metrics.RuntimeMetrics;
import gobblin.util.ConfigUtils;
import gobblin.util.ExecutorsUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import lombok.AllArgsConstructor;
import lombok.Getter;
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

  @Getter
  private final String topic;
  private final int numThreads;
  private final Config config;
  private final ConsumerConfig consumerConfig;

  /**
   * {@link MetricContext} for the consumer. Note this is instantiated when {@link #startUp()} is called, so
   * {@link com.codahale.metrics.Metric}s used by subclasses should be instantiated in the {@link #createMetrics()} method.
   */
  @Getter
  private MetricContext metricContext;
  private Counter messagesRead;
  private ConsumerConnector consumer;
  private ExecutorService executor;

  public HighLevelConsumer(String topic, Config config, int numThreads) {
    this.topic = topic;
    this.numThreads = numThreads;
    this.config = config;
    this.consumerConfig = createConsumerConfig(config);
  }

  /**
   * Called once on {@link #startUp()} to start metrics.
   */
  @VisibleForTesting
  protected void buildMetricsContextAndMetrics() {
    this.metricContext = Instrumented.getMetricContext(new gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass(), getTagsForMetrics());
    createMetrics();
  }

  @VisibleForTesting
  protected void shutdownMetrics() throws IOException {
    if (this.metricContext != null) {
      this.metricContext.close();
    }
  }

  /**
   * Instantiates {@link com.codahale.metrics.Metric}s. Called once in {@link #startUp()}. Subclasses should override
   * this method to instantiate their own metrics.
   */
  protected void createMetrics() {
    this.messagesRead = this.metricContext.counter(RuntimeMetrics.GOBBLIN_KAFKA_HIGH_LEVEL_CONSUMER_MESSAGES_READ);
  }

  /**
   * @return Tags to be applied to the {@link MetricContext} in this object. Called once in {@link #startUp()}.
   * Subclasses should override this method to add additional tags.
   */
  protected List<Tag<?>> getTagsForMetrics() {
    List<Tag<?>> tags = Lists.newArrayList();
    tags.add(new Tag<>(RuntimeMetrics.TOPIC, this.topic));
    tags.add(new Tag<>(RuntimeMetrics.GROUP_ID, this.consumerConfig.groupId()));
    return tags;
  }

  /**
   * Called every time a message is read from the stream. Implementation must be thread-safe if {@link #numThreads} is
   * set larger than 1.
   */
  protected abstract void processMessage(MessageAndMetadata<K, V> message);

  @Override
  protected void startUp() {
    buildMetricsContextAndMetrics();
    this.consumer = createConsumerConnector();

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

  protected ConsumerConfig createConsumerConfig(Config config) {
    Properties props = ConfigUtils.configToProperties(config);

    if (!props.containsKey(GROUP_ID_KEY)) {
      props.setProperty(GROUP_ID_KEY, DEFAULT_GROUP_ID);
    }
    return new ConsumerConfig(props);
  }

  protected ConsumerConnector createConsumerConnector() {
    return Consumer.createJavaConsumerConnector(this.consumerConfig);
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
    try {
      this.shutdownMetrics();
    } catch (IOException ioe) {
      log.warn("Failed to shutdown metrics for " + this.getClass().getSimpleName());
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
        HighLevelConsumer.this.messagesRead.inc();
        HighLevelConsumer.this.processMessage(message);
      }
    }
  }

}
