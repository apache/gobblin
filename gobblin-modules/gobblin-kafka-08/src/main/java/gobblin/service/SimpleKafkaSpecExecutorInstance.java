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

package gobblin.service;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.kafka.writer.Kafka08DataWriter;
import gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import gobblin.kafka.client.DecodeableKafkaRecord;
import gobblin.kafka.client.GobblinKafkaConsumerClient;
import gobblin.kafka.client.Kafka08ConsumerClient;
import gobblin.kafka.client.KafkaConsumerRecord;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstance;
import gobblin.runtime.api.SpecExecutorInstanceConsumer;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.source.extractor.extract.kafka.KafkaTopic;
import gobblin.util.CompletedFuture;
import gobblin.util.ConfigUtils;
import gobblin.writer.WriteCallback;


public class SimpleKafkaSpecExecutorInstance implements SpecExecutorInstance,
                                                        SpecExecutorInstanceProducer<Spec>, SpecExecutorInstanceConsumer<Spec>,
                                                        Closeable {
  private static final String SPEC_KAFKA_TOPIC_KEY = "spec.kafka.topic.key";
  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Splitter SPLIT_BY_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  private static final Splitter SPLIT_BY_COLON = Splitter.on(":").omitEmptyStrings().trimResults();

  // Executor Instance
  protected final Config _config;
  protected final Optional<Logger> _log;
  protected final URI _specExecutorInstanceUri;
  protected final Map<String, String> _capabilities;

  // Producer
  protected final Kafka08DataWriter<String> _kafka08Producer;

  // Consumer
  protected final GobblinKafkaConsumerClient _kafka08Consumer;
  protected final List<KafkaPartition> _partitions;
  protected final List<Long> _lowWatermark;
  protected final List<Long> _nextWatermark;
  protected final List<Long> _highWatermark;

  private Iterator<KafkaConsumerRecord> messageIterator = null;
  private int currentPartitionIdx = -1;
  private boolean isFirstRun = true;

  public SimpleKafkaSpecExecutorInstance(Config config, Optional<Logger> log) {
    _config = config;
    _log = log;
    try {
      _specExecutorInstanceUri = new URI(ConfigUtils.getString(config, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,
          "NA"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    _capabilities = Maps.newHashMap();
    if (config.hasPath(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY)) {
      String capabilitiesStr = config.getString(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY);
      List<String> capabilities = SPLIT_BY_COMMA.splitToList(capabilitiesStr);
      for (String capability : capabilities) {
        List<String> currentCapability = SPLIT_BY_COLON.splitToList(capability);
        Preconditions.checkArgument(currentCapability.size() == 2, "Only one source:destination pair is supported "
            + "per capability, found: " + currentCapability);
        _capabilities.put(currentCapability.get(0), currentCapability.get(1));
      }
    }

    // Producer
    _kafka08Producer = new Kafka08DataWriter<String>(ConfigUtils.configToProperties(config));

    // Consumer
    _kafka08Consumer = new Kafka08ConsumerClient.Factory().create(config);
    List<KafkaTopic> kafkaTopics = _kafka08Consumer.getFilteredTopics(Collections.EMPTY_LIST,
        Lists.newArrayList(Pattern.compile(SPEC_KAFKA_TOPIC_KEY)));
    _partitions = kafkaTopics.get(0).getPartitions();
    _lowWatermark = new ArrayList<Long>(_partitions.size());
    _nextWatermark = new ArrayList<Long>(_partitions.size());
    _highWatermark = new ArrayList<Long>(_partitions.size());
  }

  public SimpleKafkaSpecExecutorInstance(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public SimpleKafkaSpecExecutorInstance(Config config) {
    this(config, Optional.<Logger>absent());
  }

  /**********************************************************************************
   *
   * Spec Executor Instance basic definition.
   *
   ********************************************************************************* */

  @Override
  public URI getUri() {
    return _specExecutorInstanceUri;
  }

  @Override
  public Future<String> getDescription() {
    return new CompletedFuture<>("SimpleSpecExecutorInstance with URI: " + _specExecutorInstanceUri, null);
  }

  @Override
  public Future<Config> getConfig() {
    return new CompletedFuture<>(_config, null);
  }

  @Override
  public Future<String> getHealth() {
    return new CompletedFuture<>("Healthy", null);
  }

  @Override
  public Future<? extends Map<String, String>> getCapabilities() {
    return new CompletedFuture<>(_capabilities, null);
  }

  /**********************************************************************************
   *
   * Spec Executor Instance Producer capabilities.
   *
   ********************************************************************************* */

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    SpecExecutorInstanceDataPacket sidp = new SpecExecutorInstanceDataPacket(Verb.ADD,
        addedSpec.getUri(), addedSpec);
    return _kafka08Producer.write(gson.toJson(sidp), WriteCallback.EMPTY);
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    SpecExecutorInstanceDataPacket sidp = new SpecExecutorInstanceDataPacket(Verb.UPDATE,
        updatedSpec.getUri(), updatedSpec);
    return _kafka08Producer.write(gson.toJson(sidp), WriteCallback.EMPTY);
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI) {
    SpecExecutorInstanceDataPacket sidp = new SpecExecutorInstanceDataPacket(Verb.DELETE,
        deletedSpecURI, null);
    return _kafka08Producer.write(gson.toJson(sidp), WriteCallback.EMPTY);
  }

  @Override
  public Future<? extends List<Spec>> listSpecs() {
    throw new UnsupportedOperationException();
  }

  /**********************************************************************************
   *
   * Spec Executor Instance Consumer capabilities.
   *
   ********************************************************************************* */

  @Override
  public Future<? extends Map<Verb, Spec>> changedSpecs() {
    Map<Verb, Spec> verbSpecMap = Maps.newHashMap();
    initializeWatermarks();
    this.currentPartitionIdx = -1;
    while (!allPartitionsFinished()) {
      if (currentPartitionFinished()) {
        moveToNextPartition();
        continue;
      }
      if (this.messageIterator == null || !this.messageIterator.hasNext()) {
        try {
          this.messageIterator = fetchNextMessageBuffer();
        } catch (Exception e) {
          if (_log.isPresent()) {
            _log.get().error(String.format("Failed to fetch next message buffer for partition %s. Will skip this partition.",
                getCurrentPartition()), e);
          }
          moveToNextPartition();
          continue;
        }
        if (this.messageIterator == null || !this.messageIterator.hasNext()) {
          moveToNextPartition();
          continue;
        }
      }
      while (!currentPartitionFinished()) {
        if (!this.messageIterator.hasNext()) {
          break;
        }

        KafkaConsumerRecord nextValidMessage = this.messageIterator.next();

        // Even though we ask Kafka to give us a message buffer starting from offset x, it may
        // return a buffer that starts from offset smaller than x, so we need to skip messages
        // until we get to x.
        if (nextValidMessage.getOffset() < _nextWatermark.get(this.currentPartitionIdx)) {
          continue;
        }

        _nextWatermark.set(this.currentPartitionIdx, nextValidMessage.getNextOffset());
        try {
          SpecExecutorInstanceDataPacket record = null;
          if (nextValidMessage instanceof ByteArrayBasedKafkaRecord) {
            record = decodeRecord((ByteArrayBasedKafkaRecord)nextValidMessage);
          } else if (nextValidMessage instanceof DecodeableKafkaRecord){
            record = ((DecodeableKafkaRecord<?, SpecExecutorInstanceDataPacket>) nextValidMessage).getValue();
          } else {
            throw new IllegalStateException(
                "Unsupported KafkaConsumerRecord type. The returned record can either be ByteArrayBasedKafkaRecord"
                    + " or DecodeableKafkaRecord");
          }
          verbSpecMap.put(record._verb, record._spec);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    }

    return new CompletedFuture(verbSpecMap, null);
  }

  private void initializeWatermarks() {
    initializeLowWatermarks();
    initializeHighWatermarks();
  }

  private void initializeLowWatermarks() {
    try {
      int i=0;
      for (KafkaPartition kafkaPartition : _partitions) {
        if (isFirstRun) {
          long earliestOffset = _kafka08Consumer.getEarliestOffset(kafkaPartition);
          _lowWatermark.set(i, earliestOffset);
        } else {
          _lowWatermark.set(i, _highWatermark.get(i));
        }
        i++;
      }
      isFirstRun = false;
    } catch (KafkaOffsetRetrievalFailureException e) {
       throw new RuntimeException(e);
    }
  }

  private void initializeHighWatermarks() {
    try {
      int i=0;
      for (KafkaPartition kafkaPartition : _partitions) {
        long latestOffset = _kafka08Consumer.getLatestOffset(kafkaPartition);
        _highWatermark.set(i, latestOffset);
        i++;
      }
    } catch (KafkaOffsetRetrievalFailureException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean allPartitionsFinished() {
    return this.currentPartitionIdx >= _nextWatermark.size();
  }

  private boolean currentPartitionFinished() {
    if (this.currentPartitionIdx == -1) {
      return true;
    } else if (_nextWatermark.get(this.currentPartitionIdx) >= _highWatermark.get(this.currentPartitionIdx)) {
      return true;
    } else {
      return false;
    }
  }

  private int moveToNextPartition() {
    this.messageIterator = null;
    return this.currentPartitionIdx ++;
  }

  private KafkaPartition getCurrentPartition() {
    return _partitions.get(this.currentPartitionIdx);
  }

  private Iterator<KafkaConsumerRecord> fetchNextMessageBuffer() {
    return _kafka08Consumer.consume(_partitions.get(this.currentPartitionIdx),
        _nextWatermark.get(this.currentPartitionIdx), _highWatermark.get(this.currentPartitionIdx));
  }

  private SpecExecutorInstanceDataPacket decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) throws IOException {
    return gson.fromJson(new String(kafkaConsumerRecord.getMessageBytes(), Charset.forName("UTF-8")), SpecExecutorInstanceDataPacket.class);
  }

  /**********************************************************************************
   *
   * Misc housekeeping.
   *
   ********************************************************************************* */


  @Override
  public void close() throws IOException {
    _kafka08Producer.close();
    _kafka08Consumer.close();
  }

  public static class SpecExecutorInstanceDataPacket {

    private Verb _verb;
    private URI _uri;
    private Spec _spec;

    public SpecExecutorInstanceDataPacket(Verb verb, URI uri, Spec spec) {
      _verb = verb;
      _uri = uri;
      _spec = spec;
    }
  }
}
