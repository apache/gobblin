/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import gobblin.configuration.State;
import gobblin.source.extractor.extract.EventBasedSource;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

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

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;


/**
 * Wrapper class that contains two alternative Kakfa APIs: an old low-level Scala-based API, and a new API.
 * The new API has not been implemented since it's not ready to be open sourced.
 *
 * @author ziliu
 */
public class KafkaWrapper implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaWrapper.class);

  private static final String USE_NEW_KAFKA_API = "use.new.kafka.api";
  private static final boolean DEFAULT_USE_NEW_KAFKA_API = false;
  private static final String KAFKA_BROKERS = "kafka.brokers";

  private final List<String> brokers;
  private final KafkaAPI kafkaAPI;

  private final boolean useNewKafkaAPI;

  private static class Builder {
    private boolean useNewKafkaAPI = DEFAULT_USE_NEW_KAFKA_API;
    private List<String> brokers = Lists.newArrayList();

    private Builder withNewKafkaAPI() {
      this.useNewKafkaAPI = true;
      return this;
    }

    private Builder withBrokers(List<String> brokers) {
      for (String broker : brokers) {
        Preconditions.checkArgument(broker.matches(".+:\\d+"),
            String.format("Invalid broker: %s. Must be in the format of address:port.", broker));
      }
      this.brokers = Lists.newArrayList(brokers);
      return this;
    }

    private KafkaWrapper build() {
      Preconditions.checkArgument(!brokers.isEmpty(), "Need to specify at least one Kafka broker.");
      return new KafkaWrapper(this);
    }
  }

  private KafkaWrapper(Builder builder) {
    this.useNewKafkaAPI = builder.useNewKafkaAPI;
    this.brokers = builder.brokers;
    this.kafkaAPI = getKafkaAPI();
  }

  /**
   * Create a KafkaWrapper based on the given type of Kafka API and list of Kafka brokers.
   *
   * @param state A {@link State} object that should contain a list of comma separated Kafka brokers
   * in property "kafka.brokers". It may optionally specify whether to use the new Kafka API by setting
   * use.new.kafka.api=true.
   */
  public static KafkaWrapper create(State state) {
    Preconditions.checkNotNull(state.getProp(KAFKA_BROKERS), "Need to specify at least one Kafka broker.");
    KafkaWrapper.Builder builder = new KafkaWrapper.Builder();
    if (state.getPropAsBoolean(USE_NEW_KAFKA_API, DEFAULT_USE_NEW_KAFKA_API)) {
      builder = builder.withNewKafkaAPI();
    }
    return builder.withBrokers(state.getPropAsList(KAFKA_BROKERS)).build();
  }

  public List<String> getBrokers() {
    return this.brokers;
  }

  public List<KafkaTopic> getFilteredTopics(Set<String> blacklist, Set<String> whitelist) {
    return this.kafkaAPI.getFilteredTopics(blacklist, whitelist);
  }

  public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    return this.kafkaAPI.getEarliestOffset(partition);
  }

  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    return this.kafkaAPI.getLatestOffset(partition);
  }

  public Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset, long maxOffset) {
    return this.kafkaAPI.fetchNextMessageBuffer(partition, nextOffset, maxOffset);
  }

  private KafkaAPI getKafkaAPI() {
    if (this.useNewKafkaAPI) {
      return new KafkaNewAPI();
    } else {
      return new KafkaOldAPI();
    }
  }

  @Override
  public void close() throws IOException {
    this.kafkaAPI.close();
  }

  private abstract class KafkaAPI implements Closeable {
    protected abstract List<KafkaTopic> getFilteredTopics(Set<String> blacklist, Set<String> whitelist);

    protected abstract long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException;

    protected abstract long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException;

    protected abstract Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset,
        long maxOffset);
  }

  /**
   * Wrapper for the old low-level Scala-based Kafka API.
   */
  private class KafkaOldAPI extends KafkaAPI {
    private static final int DEFAULT_KAFKA_TIMEOUT_VALUE = 30000;
    private static final int DEFAULT_KAFKA_BUFFER_SIZE = 1024 * 1024;
    private static final String DEFAULT_KAFKA_CLIENT_NAME = "kafka-old-api";
    private static final int DEFAULT_KAFKA_FETCH_REQUEST_CORRELATION_ID = -1;
    private static final int DEFAULT_KAFKA_FETCH_REQUEST_MIN_BYTES = 1024;
    private static final int NUM_TRIES_FETCH_TOPIC = 3;
    private static final int NUM_TRIES_FETCH_OFFSET = 3;

    private final ConcurrentMap<String, SimpleConsumer> activeConsumers = Maps.newConcurrentMap();

    @Override
    public List<KafkaTopic> getFilteredTopics(Set<String> blacklist, Set<String> whitelist) {
      List<TopicMetadata> topicMetadataList = getFilteredMetadataList(blacklist, whitelist);

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
        partitions.add(new KafkaPartition.Builder().withId(partitionMetadata.partitionId())
            .withTopicName(topicMetadata.topic()).withLeaderId(partitionMetadata.leader().id())
            .withLeaderHostAndPort(partitionMetadata.leader().host(), partitionMetadata.leader().port()).build());
      }
      return partitions;
    }

    private List<TopicMetadata> getFilteredMetadataList(Set<String> blacklist, Set<String> whitelist) {
      List<TopicMetadata> filteredTopicMetadataList = Lists.newArrayList();

      //Try all brokers one by one, until successfully retrieved topic metadata (topicMetadataList is non-null)
      for (String broker : KafkaWrapper.this.getBrokers()) {
        filteredTopicMetadataList = fetchTopicMetadataFromBroker(broker, blacklist, whitelist);
        if (filteredTopicMetadataList != null) {
          return filteredTopicMetadataList;
        }
      }

      throw new RuntimeException(
          "Fetching topic metadata from all brokers failed. See log warning for more information.");
    }

    private List<TopicMetadata> fetchTopicMetadataFromBroker(String broker, Set<String> blacklist, Set<String> whitelist) {

      List<TopicMetadata> topicMetadataList = fetchTopicMetadataFromBroker(broker);
      if (topicMetadataList == null) {
        return null;
      }

      List<TopicMetadata> filteredTopicMetadataList = Lists.newArrayList();
      for (TopicMetadata topicMetadata : topicMetadataList) {
        if (EventBasedSource.survived(topicMetadata.topic(), blacklist, whitelist)) {
          filteredTopicMetadataList.add(topicMetadata);
        }
      }
      return filteredTopicMetadataList;
    }

    private List<TopicMetadata> fetchTopicMetadataFromBroker(String broker, String... selectedTopics) {
      LOG.info(String.format("Fetching topic metadata from broker %s", broker));
      SimpleConsumer consumer = null;
      try {
        consumer = getSimpleConsumer(broker);
        for (int i = 0; i < NUM_TRIES_FETCH_TOPIC; i++) {
          try {
            return consumer.send(new TopicMetadataRequest(Arrays.asList(selectedTopics))).topicsMetadata();
          } catch (Exception e) {
            LOG.warn(String.format("Fetching topic metadata from broker %s has failed %d times.", broker, i + 1), e);
            try {
              Thread.sleep((long) ((i + Math.random()) * 1000));
            } catch (InterruptedException e2) {
              LOG.warn("Caught InterruptedException: " + e2);
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
      } else {
        SimpleConsumer consumer = this.createSimpleConsumer(broker);
        this.activeConsumers.putIfAbsent(broker, consumer);
        return consumer;
      }
    }

    private SimpleConsumer getSimpleConsumer(HostAndPort hostAndPort) {
      return this.getSimpleConsumer(hostAndPort.toString());
    }

    private SimpleConsumer createSimpleConsumer(String broker) {
      List<String> hostPort = Splitter.on(':').trimResults().omitEmptyStrings().splitToList(broker);
      return createSimpleConsumer(hostPort.get(0), Integer.parseInt(hostPort.get(1)));
    }

    private SimpleConsumer createSimpleConsumer(String host, int port) {
      return new SimpleConsumer(host, port, DEFAULT_KAFKA_TIMEOUT_VALUE, DEFAULT_KAFKA_BUFFER_SIZE,
          DEFAULT_KAFKA_CLIENT_NAME);
    }

    @Override
    protected long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
      Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
          Collections.singletonMap(new TopicAndPartition(partition.getTopicName(), partition.getId()),
              new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
      return getOffset(partition, offsetRequestInfo);
    }

    @Override
    protected long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
      Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
          Collections.singletonMap(new TopicAndPartition(partition.getTopicName(), partition.getId()),
              new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
      return getOffset(partition, offsetRequestInfo);
    }

    private long getOffset(KafkaPartition partition,
        Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo)
        throws KafkaOffsetRetrievalFailureException {
      SimpleConsumer consumer = this.getSimpleConsumer(partition.getLeader().getHostAndPort());
      for (int i = 0; i < NUM_TRIES_FETCH_OFFSET; i++) {
        try {
          OffsetResponse offsetResponse =
              consumer.getOffsetsBefore(new OffsetRequest(offsetRequestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                  DEFAULT_KAFKA_CLIENT_NAME));
          if (offsetResponse.hasError()) {
            throw new RuntimeException("offsetReponse has error: "
                + offsetResponse.errorCode(partition.getTopicName(), partition.getId()));
          }
          return offsetResponse.offsets(partition.getTopicName(), partition.getId())[0];
        } catch (Exception e) {
          LOG.warn(String.format("Fetching offset for partition %s has failed %d time(s). Reason: %s", partition,
              i + 1, e));
          if (i < NUM_TRIES_FETCH_OFFSET - 1) {
            try {
              Thread.sleep((long) ((i + Math.random()) * 1000));
            } catch (InterruptedException e2) {
              LOG.error("Caught interrupted exception between retries of getting latest offsets. " + e2);
            }
          }
        }
      }
      throw new KafkaOffsetRetrievalFailureException(String.format("Fetching offset for partition %s has failed.",
          partition));
    }

    @Override
    protected Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset,
        long maxOffset) {
      if (nextOffset > maxOffset) {
        return null;
      }

      FetchRequest fetchRequest = createFetchRequest(partition, nextOffset);

      try {
        FetchResponse fetchResponse = getFetchResponseForFetchRequest(fetchRequest, partition);
        return getIteratorFromFetchResponse(fetchResponse, partition);
      } catch (Exception e) {
        LOG.warn(String
            .format("Fetch message buffer for partition %s has failed: %s. Will refresh topic metadata and retry",
                partition, e));
        return refreshTopicMetadataAndRetryFetch(partition, fetchRequest);
      }
    }

    private synchronized FetchResponse getFetchResponseForFetchRequest(FetchRequest fetchRequest,
        KafkaPartition partition) {
      SimpleConsumer consumer = getSimpleConsumer(partition.getLeader().getHostAndPort());

      FetchResponse fetchResponse = consumer.fetch(fetchRequest);
      if (fetchResponse.hasError()) {
        throw new RuntimeException(String.format("error code %d",
            fetchResponse.errorCode(partition.getTopicName(), partition.getId())));
      }
      return fetchResponse;
    }

    private Iterator<MessageAndOffset> getIteratorFromFetchResponse(FetchResponse fetchResponse,
        KafkaPartition partition) {
      try {
        ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(partition.getTopicName(), partition.getId());
        return messageBuffer.iterator();
      } catch (Exception e) {
        LOG.warn(String.format("Failed to retrieve next message buffer for partition %s: %s."
            + "The remainder of this partition will be skipped.", partition, e));
        return null;
      }
    }

    private Iterator<MessageAndOffset> refreshTopicMetadataAndRetryFetch(KafkaPartition partition,
        FetchRequest fetchRequest) {
      try {
        refreshTopicMetadata(partition);
        FetchResponse fetchResponse = getFetchResponseForFetchRequest(fetchRequest, partition);
        return getIteratorFromFetchResponse(fetchResponse, partition);
      } catch (Exception e) {
        LOG.warn(String.format("Fetch message buffer for partition %s has failed: %s. This partition will be skipped.",
            partition, e));
        return null;
      }
    }

    private void refreshTopicMetadata(KafkaPartition partition) {
      for (String broker : KafkaWrapper.this.getBrokers()) {
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
      PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(nextOffset, DEFAULT_KAFKA_BUFFER_SIZE);
      Map<TopicAndPartition, PartitionFetchInfo> fetchInfo =
          Collections.singletonMap(topicAndPartition, partitionFetchInfo);
      return new FetchRequest(DEFAULT_KAFKA_FETCH_REQUEST_CORRELATION_ID, DEFAULT_KAFKA_CLIENT_NAME,
          DEFAULT_KAFKA_TIMEOUT_VALUE, DEFAULT_KAFKA_FETCH_REQUEST_MIN_BYTES, fetchInfo);
    }

    @Override
    public void close() throws IOException {
      int numOfConsumersNotClosed = 0;

      for (SimpleConsumer consumer : this.activeConsumers.values()) {
        if (consumer != null) {
          try {
            consumer.close();
          } catch (Exception e) {
            LOG.warn(String.format("Failed to close Kafka Consumer %s:%d", consumer.host(), consumer.port()));
            numOfConsumersNotClosed++;
          }
        }
      }
      this.activeConsumers.clear();
      if (numOfConsumersNotClosed > 0) {
        throw new IOException(numOfConsumersNotClosed + " consumer(s) failed to close.");
      }
    }
  }

  /**
   * Wrapper for the new Kafka API.
   */
  private class KafkaNewAPI extends KafkaAPI {

    @Override
    public List<KafkaTopic> getFilteredTopics(Set<String> blacklist, Set<String> whitelist) {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    protected long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    protected long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    public void close() throws IOException {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    protected Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset,
        long maxOffset) {
      throw new NotImplementedException("kafka new API has not been implemented");
    }
  }

}
