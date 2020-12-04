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
package org.apache.gobblin.kafka.client;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.codahale.metrics.Metric;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.DatasetFilterUtils;

/**
 * A simplified, generic wrapper client to communicate with Kafka. This class is (AND MUST never) depend on classes
 * defined in kafka-clients library. The request and response objects are defined in gobblin. This allows gobblin
 * sources and extractors to work with different versions of kafka. Concrete classes implementing this interface use a
 * specific version of kafka-client library.
 *
 * <p>
 * This simplified client interface supports consumer operations required by gobblin to pull from kafka. Most of the APIs
 * are migrated from the legacy gobblin.source.extractor.extract.kafka.KafkaWrapper$KafkaAPI
 * </p>
 */
public interface GobblinKafkaConsumerClient extends Closeable {

  /**
   * Get a list of {@link KafkaTopic}s satisfy <code>blacklist</code> and <code>whitelist</code> patterns
   * @param blacklist - List of regex patterns that need to be blacklisted
   * @param whitelist - List of regex patterns that need to be whitelisted
   *
   * @see DatasetFilterUtils#survived(String, List, List)
   *
   */
  public List<KafkaTopic> getFilteredTopics(List<Pattern> blacklist, List<Pattern> whitelist);

  /**
   * Get the earliest available offset for a <code>partition</code>
   *
   * @param partition for which earliest offset is retrieved
   *
   * @throws UnsupportedOperationException - If the underlying kafka-client does not support getting earliest offset
   */
  public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException;

  /**
   * Get the latest available offset for a <code>partition</code>
   *
   * @param partition for which latest offset is retrieved
   *
   * @throws UnsupportedOperationException - If the underlying kafka-client does not support getting latest offset
   */
  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException;

  /**
   * Get the latest available offset for a {@link Collection} of {@link KafkaPartition}s. NOTE: The default implementation
   * is not efficient i.e. it will make a getLatestOffset() call for every {@link KafkaPartition}. Individual implementations
   * of {@link GobblinKafkaConsumerClient} should override this method to use more advanced APIs of the underlying KafkaConsumer
   * to retrieve the latest offsets for a collection of partitions.
   *
   * @param partitions for which latest offset is retrieved
   *
   * @throws KafkaOffsetRetrievalFailureException - If the underlying kafka-client does not support getting latest offset
   */
  public default Map<KafkaPartition, Long> getLatestOffsets(Collection<KafkaPartition> partitions)
      throws KafkaOffsetRetrievalFailureException {
    Map<KafkaPartition, Long> offsetMap = Maps.newHashMap();
    for (KafkaPartition partition: partitions) {
      offsetMap.put(partition, getLatestOffset(partition));
    }
    return offsetMap;
  }

  /**
   * API to consume records from kakfa starting from <code>nextOffset</code> till <code>maxOffset</code>.
   * If <code>nextOffset</code> is greater than <code>maxOffset</code>, returns a null.
   * <code>nextOffset</code>
   * <p>
   *  <b>NOTE:</b> If the underlying kafka-client version does not support
   *  reading message till an end offset, all available messages are read, <code>maxOffset</code> is ignored.
   * </p>
   *
   * @param partition whose messages are read
   * @param nextOffset to being reading
   * @param maxOffset to stop reading (Iff underlying client supports)
   *
   * @return An {@link Iterator} of {@link KafkaConsumerRecord}s
   */
  public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset);

  /**
   * API to consume records from kakfa
   * @return
   */
  default Iterator<KafkaConsumerRecord> consume() {
    return Collections.emptyIterator();
  }

  /**
   * Subscribe to a topic
   * @param topic
   */
  default void subscribe(String topic) {
    return;
  }

  /**
   * Subscribe to a topic along with a GobblinKafkaRebalanceListener
   * @param topic
   */
  default void subscribe(String topic, GobblinConsumerRebalanceListener listener) {
    return;
  }
  /**
   * API to return underlying Kafka consumer metrics. The individual implementations must translate
   * org.apache.kafka.common.Metric to Coda Hale Metrics. A typical use case for reporting the consumer metrics
   * will call this method inside a scheduled thread.
   * @return
   */
  public default Map<String, Metric> getMetrics() {
    return Maps.newHashMap();
  }

  /**
   * Commit offsets manually to Kafka asynchronously
   */
  default void commitOffsetsAsync(Map<KafkaPartition, Long> partitionOffsets) {
    return;
  }

  /**
   * Commit offsets manually to Kafka synchronously
   */
  default void commitOffsetsSync(Map<KafkaPartition, Long> partitionOffsets) {
    return;
  }

  /**
   * returns the last committed offset for a KafkaPartition
   * @param partition
   * @return last committed offset or -1 for invalid KafkaPartition
   */
  default long committed(KafkaPartition partition) {
    return -1L;
  }

  public default void assignAndSeek(List<KafkaPartition> topicPartitions, Map<KafkaPartition, LongWatermark> topicWatermarksMap) { return; }

  /**
   * A factory to create {@link GobblinKafkaConsumerClient}s
   */
  public interface GobblinKafkaConsumerClientFactory {
    /**
     * Creates a new {@link GobblinKafkaConsumerClient} for <code>config</code>
     *
     */
    public GobblinKafkaConsumerClient create(Config config);
  }
}
