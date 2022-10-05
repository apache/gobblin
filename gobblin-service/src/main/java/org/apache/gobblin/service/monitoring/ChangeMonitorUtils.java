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

package org.apache.gobblin.service.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;

@Slf4j
public final class ChangeMonitorUtils {
  private ChangeMonitorUtils() {
    return;
  }

  /**
   * The consumer client will assign itself to all partitions for this topic and consume from its latest offset.
   * @param topic Kafka topic name
   * @param gobblinKafkaConsumerClient Kafka consumer in use
   */
  public static void assignTopicPartitionsHelper(String topic, GobblinKafkaConsumerClient gobblinKafkaConsumerClient) {
    List<KafkaTopic> kafkaTopicList = gobblinKafkaConsumerClient.getFilteredTopics(Collections.EMPTY_LIST,
        Lists.newArrayList(Pattern.compile(topic)));

    List<KafkaPartition> kafkaPartitions = new ArrayList();
    for (KafkaTopic kafkaTopic : kafkaTopicList) {
      kafkaPartitions.addAll(kafkaTopic.getPartitions());
    }

    Map<KafkaPartition, LongWatermark> partitionLongWatermarkMap = new HashMap<>();
    for (KafkaPartition partition : kafkaPartitions) {
      try {
        partitionLongWatermarkMap.put(partition, new LongWatermark(gobblinKafkaConsumerClient.getLatestOffset(partition)));
      } catch (KafkaOffsetRetrievalFailureException e) {
        log.warn("Failed to retrieve latest Kafka offset, consuming from beginning for partition {} due to {}",
            partition, e);
        partitionLongWatermarkMap.put(partition, new LongWatermark(0L));
      }
    }
    gobblinKafkaConsumerClient.assignAndSeek(kafkaPartitions, partitionLongWatermarkMap);
  }

  /**
   * Performs checks for duplicate messages and heartbeat operation prior to processing a message. Returns true if
   * the pre-conditions above don't apply and we should proceed processing the change event
   */
  public static boolean shouldProcessMessage(String changeIdentifier, LoadingCache<String, String> cache,
      String operation, String timestamp) {
    // If we've already processed a message with this timestamp and key before then skip duplicate message
    if (cache.getIfPresent(changeIdentifier) != null) {
      log.debug("Duplicate change event with identifier {}", changeIdentifier);
      return false;
    }

    // If event is a heartbeat type then log it and skip processing
    if (operation.equals("HEARTBEAT")) {
      log.debug("Received heartbeat message from time {}", timestamp);
      return false;
    }

    return true;
  }
}
