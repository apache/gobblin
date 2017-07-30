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
package gobblin.kafka.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.typesafe.config.Config;

import gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.source.extractor.extract.kafka.KafkaTopic;
import gobblin.util.ConfigUtils;
import gobblin.util.DatasetFilterUtils;

/**
 * A {@link GobblinKafkaConsumerClient} that uses kafka 08 scala consumer client. All the code has been moved from the
 * legacy gobblin.source.extractor.extract.kafka.KafkaWrapper's KafkaOldApi
 */
@Slf4j
public class Kafka08ConsumerClient extends AbstractBaseKafkaConsumerClient {

  public static final String CONFIG_PREFIX = AbstractBaseKafkaConsumerClient.CONFIG_PREFIX;
  public static final String CONFIG_KAFKA_BUFFER_SIZE_BYTES = CONFIG_PREFIX + "bufferSizeBytes";
  public static final int CONFIG_KAFKA_BUFFER_SIZE_BYTES_DEFAULT = 1024 * 1024; // 1MB
  public static final String CONFIG_KAFKA_CLIENT_NAME = CONFIG_PREFIX + "clientName";
  public static final String CONFIG_KAFKA_CLIENT_NAME_DEFAULT = "gobblin-kafka";
  public static final String CONFIG_KAFKA_FETCH_REQUEST_CORRELATION_ID = CONFIG_PREFIX + "fetchCorrelationId";
  private static final int CONFIG_KAFKA_FETCH_REQUEST_CORRELATION_ID_DEFAULT = -1;
  public static final String CONFIG_KAFKA_FETCH_TOPIC_NUM_TRIES = CONFIG_PREFIX + "fetchTopicNumTries";
  private static final int CONFIG_KAFKA_FETCH_TOPIC_NUM_TRIES_DEFAULT = 3;
  public static final String CONFIG_KAFKA_FETCH_OFFSET_NUM_TRIES = CONFIG_PREFIX + "fetchOffsetNumTries";
  private static final int CONFIG_KAFKA_FETCH_OFFSET_NUM_TRIES_DEFAULT = 3;

  private final int bufferSize;
  private final String clientName;
  private final int fetchCorrelationId;
  private final int fetchTopicRetries;
  private final int fetchOffsetRetries;

  private final ConcurrentMap<String, SimpleConsumer> activeConsumers = Maps.newConcurrentMap();

  private Kafka08ConsumerClient(Config config) {
    super(config);
    bufferSize = ConfigUtils.getInt(config, CONFIG_KAFKA_BUFFER_SIZE_BYTES, CONFIG_KAFKA_BUFFER_SIZE_BYTES_DEFAULT);
    clientName = ConfigUtils.getString(config, CONFIG_KAFKA_CLIENT_NAME, CONFIG_KAFKA_CLIENT_NAME_DEFAULT);
    fetchCorrelationId =
        ConfigUtils.getInt(config, CONFIG_KAFKA_FETCH_REQUEST_CORRELATION_ID,
            CONFIG_KAFKA_FETCH_REQUEST_CORRELATION_ID_DEFAULT);
    fetchTopicRetries =
        ConfigUtils.getInt(config, CONFIG_KAFKA_FETCH_TOPIC_NUM_TRIES, CONFIG_KAFKA_FETCH_TOPIC_NUM_TRIES_DEFAULT);
    fetchOffsetRetries =
        ConfigUtils.getInt(config, CONFIG_KAFKA_FETCH_OFFSET_NUM_TRIES, CONFIG_KAFKA_FETCH_OFFSET_NUM_TRIES_DEFAULT);
  }

  @Override
  public List<KafkaTopic> getTopics() {
    List<TopicMetadata> topicMetadataList = getFilteredMetadataList();

    List<KafkaTopic> filteredTopics = Lists.newArrayList();
    for (TopicMetadata topicMetadata : topicMetadataList) {
      List<KafkaPartition> partitions = getPartitionsForTopic(topicMetadata);
      filteredTopics.add(new KafkaTopic(topicMetadata.topic(), partitions));
    }
    return filteredTopics;
  }

  private List<KafkaPartition> getPartitionsForTopic(TopicMetadata topicMetadata) {
    List<KafkaPartition> partitions = Lists.newArrayList();

    for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
      if (null == partitionMetadata) {
        log.error("Ignoring topic with null partition metadata " + topicMetadata.topic());
        return Collections.emptyList();
      }
      if (null == partitionMetadata.leader()) {
        log.error("Ignoring topic with null partition leader " + topicMetadata.topic() + " metatada="
            + partitionMetadata);
        return Collections.emptyList();
      }
      partitions.add(new KafkaPartition.Builder().withId(partitionMetadata.partitionId())
          .withTopicName(topicMetadata.topic()).withLeaderId(partitionMetadata.leader().id())
          .withLeaderHostAndPort(partitionMetadata.leader().host(), partitionMetadata.leader().port()).build());
    }
    return partitions;
  }

  private List<TopicMetadata> getFilteredMetadataList() {
    //Try all brokers one by one, until successfully retrieved topic metadata (topicMetadataList is non-null)
    for (String broker : this.brokers) {
      List<TopicMetadata> filteredTopicMetadataList = fetchTopicMetadataFromBroker(broker);
      if (filteredTopicMetadataList != null) {
        return filteredTopicMetadataList;
      }
    }

    throw new RuntimeException("Fetching topic metadata from all brokers failed. See log warning for more information.");
  }

  private List<TopicMetadata> fetchTopicMetadataFromBroker(String broker, String... selectedTopics) {
    log.info(String.format("Fetching topic metadata from broker %s", broker));
    SimpleConsumer consumer = null;
    try {
      consumer = getSimpleConsumer(broker);
      for (int i = 0; i < this.fetchTopicRetries; i++) {
        try {
          return consumer.send(new TopicMetadataRequest(Arrays.asList(selectedTopics))).topicsMetadata();
        } catch (Exception e) {
          log.warn(String.format("Fetching topic metadata from broker %s has failed %d times.", broker, i + 1), e);
          try {
            Thread.sleep((long) ((i + Math.random()) * 1000));
          } catch (InterruptedException e2) {
            log.warn("Caught InterruptedException: " + e2);
          }
        }
      }
    } finally {
      if (consumer != null) {
        consumer.close();
      }
    }
    return null;
  }

  private SimpleConsumer getSimpleConsumer(String broker) {
    if (this.activeConsumers.containsKey(broker)) {
      return this.activeConsumers.get(broker);
    }
    SimpleConsumer consumer = this.createSimpleConsumer(broker);
    this.activeConsumers.putIfAbsent(broker, consumer);
    return consumer;
  }

  private SimpleConsumer getSimpleConsumer(HostAndPort hostAndPort) {
    return this.getSimpleConsumer(hostAndPort.toString());
  }

  private SimpleConsumer createSimpleConsumer(String broker) {
    List<String> hostPort = Splitter.on(':').trimResults().omitEmptyStrings().splitToList(broker);
    return createSimpleConsumer(hostPort.get(0), Integer.parseInt(hostPort.get(1)));
  }

  private SimpleConsumer createSimpleConsumer(String host, int port) {
    return new SimpleConsumer(host, port, this.socketTimeoutMillis, this.bufferSize, this.clientName);
  }

  @Override
  public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
        Collections.singletonMap(new TopicAndPartition(partition.getTopicName(), partition.getId()),
            new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
    return getOffset(partition, offsetRequestInfo);
  }

  @Override
  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
        Collections.singletonMap(new TopicAndPartition(partition.getTopicName(), partition.getId()),
            new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
    return getOffset(partition, offsetRequestInfo);
  }

  private long getOffset(KafkaPartition partition, Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo)
      throws KafkaOffsetRetrievalFailureException {
    SimpleConsumer consumer = this.getSimpleConsumer(partition.getLeader().getHostAndPort());
    for (int i = 0; i < this.fetchOffsetRetries; i++) {
      try {
        OffsetResponse offsetResponse =
            consumer.getOffsetsBefore(new OffsetRequest(offsetRequestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                this.clientName));
        if (offsetResponse.hasError()) {
          throw new RuntimeException("offsetReponse has error: "
              + offsetResponse.errorCode(partition.getTopicName(), partition.getId()));
        }
        return offsetResponse.offsets(partition.getTopicName(), partition.getId())[0];
      } catch (Exception e) {
        log.warn(String.format("Fetching offset for partition %s has failed %d time(s). Reason: %s", partition, i + 1,
            e));
        if (i < this.fetchOffsetRetries - 1) {
          try {
            Thread.sleep((long) ((i + Math.random()) * 1000));
          } catch (InterruptedException e2) {
            log.error("Caught interrupted exception between retries of getting latest offsets. " + e2);
          }
        }
      }
    }
    throw new KafkaOffsetRetrievalFailureException(String.format("Fetching offset for partition %s has failed.",
        partition));
  }

  @Override
  public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {
    if (nextOffset > maxOffset) {
      return null;
    }

    FetchRequest fetchRequest = createFetchRequest(partition, nextOffset);

    try {
      FetchResponse fetchResponse = getFetchResponseForFetchRequest(fetchRequest, partition);
      return getIteratorFromFetchResponse(fetchResponse, partition);
    } catch (Exception e) {
      log.warn(String.format(
          "Fetch message buffer for partition %s has failed: %s. Will refresh topic metadata and retry", partition, e));
      return refreshTopicMetadataAndRetryFetch(partition, fetchRequest);
    }
  }

  private synchronized FetchResponse getFetchResponseForFetchRequest(FetchRequest fetchRequest, KafkaPartition partition) {
    SimpleConsumer consumer = getSimpleConsumer(partition.getLeader().getHostAndPort());

    FetchResponse fetchResponse = consumer.fetch(fetchRequest);
    if (fetchResponse.hasError()) {
      throw new RuntimeException(String.format("error code %d",
          fetchResponse.errorCode(partition.getTopicName(), partition.getId())));
    }
    return fetchResponse;
  }

  private Iterator<KafkaConsumerRecord> getIteratorFromFetchResponse(FetchResponse fetchResponse, KafkaPartition partition) {
    try {
      ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(partition.getTopicName(), partition.getId());
      return Iterators.transform(messageBuffer.iterator(),
          new Function<kafka.message.MessageAndOffset, KafkaConsumerRecord>() {
            @Override
            public KafkaConsumerRecord apply(kafka.message.MessageAndOffset input) {
              return new Kafka08ConsumerRecord(input);
            }
          });
    } catch (Exception e) {
      log.warn(String.format("Failed to retrieve next message buffer for partition %s: %s."
          + "The remainder of this partition will be skipped.", partition, e));
      return null;
    }
  }

  private Iterator<KafkaConsumerRecord> refreshTopicMetadataAndRetryFetch(KafkaPartition partition,
      FetchRequest fetchRequest) {
    try {
      refreshTopicMetadata(partition);
      FetchResponse fetchResponse = getFetchResponseForFetchRequest(fetchRequest, partition);
      return getIteratorFromFetchResponse(fetchResponse, partition);
    } catch (Exception e) {
      log.warn(String.format("Fetch message buffer for partition %s has failed: %s. This partition will be skipped.",
          partition, e));
      return null;
    }
  }

  private void refreshTopicMetadata(KafkaPartition partition) {
    for (String broker : this.brokers) {
      List<TopicMetadata> topicMetadataList = fetchTopicMetadataFromBroker(broker, partition.getTopicName());
      if (topicMetadataList != null && !topicMetadataList.isEmpty()) {
        TopicMetadata topicMetadata = topicMetadataList.get(0);
        for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
          if (partitionMetadata.partitionId() == partition.getId()) {
            partition.setLeader(partitionMetadata.leader().id(), partitionMetadata.leader().host(), partitionMetadata
                .leader().port());
            break;
          }
        }
        break;
      }
    }
  }

  private FetchRequest createFetchRequest(KafkaPartition partition, long nextOffset) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(partition.getTopicName(), partition.getId());
    PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(nextOffset, this.bufferSize);
    Map<TopicAndPartition, PartitionFetchInfo> fetchInfo =
        Collections.singletonMap(topicAndPartition, partitionFetchInfo);
    return new FetchRequest(this.fetchCorrelationId, this.clientName, this.fetchTimeoutMillis, this.fetchMinBytes,
        fetchInfo);
  }

  @Override
  public void close() throws IOException {
    int numOfConsumersNotClosed = 0;

    for (SimpleConsumer consumer : this.activeConsumers.values()) {
      if (consumer != null) {
        try {
          consumer.close();
        } catch (Exception e) {
          log.warn(String.format("Failed to close Kafka Consumer %s:%d", consumer.host(), consumer.port()));
          numOfConsumersNotClosed++;
        }
      }
    }
    this.activeConsumers.clear();
    if (numOfConsumersNotClosed > 0) {
      throw new IOException(numOfConsumersNotClosed + " consumer(s) failed to close.");
    }
  }

  public static class Factory implements GobblinKafkaConsumerClientFactory {
    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      return new Kafka08ConsumerClient(config);
    }
  }

  public static class Kafka08ConsumerRecord extends BaseKafkaConsumerRecord implements ByteArrayBasedKafkaRecord {

    private final MessageAndOffset messageAndOffset;

    public Kafka08ConsumerRecord(MessageAndOffset messageAndOffset) {
      super(messageAndOffset.offset(), messageAndOffset.message().size());
      this.messageAndOffset = messageAndOffset;
    }

    @Override
    public byte[] getMessageBytes() {
      return getBytes(this.messageAndOffset.message().payload());
    }

    @Override
    public byte[] getKeyBytes() {
      return getBytes(this.messageAndOffset.message().key());
    }

    private static byte[] getBytes(ByteBuffer buf) {
      byte[] bytes = null;
      if (buf != null) {
        int size = buf.remaining();
        bytes = new byte[size];
        buf.get(bytes, buf.position(), size);
      }
      return bytes;
    }

  }
}
