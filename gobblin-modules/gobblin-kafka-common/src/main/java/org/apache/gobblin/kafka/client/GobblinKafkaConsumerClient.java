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
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import com.typesafe.config.Config;

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
   * API to consume records from kakfa starting from <code>nextOffset</code> till <code>maxOffset</code>.
   * If <code>maxOffset</code> is greater than <code>nextOffset</code>, returns a null.
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
