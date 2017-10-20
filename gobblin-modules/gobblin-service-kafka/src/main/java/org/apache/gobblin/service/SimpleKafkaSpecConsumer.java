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

package org.apache.gobblin.service;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecConsumer;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.job_spec.AvroJobSpec;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.SimpleKafkaSpecExecutor.VERB_KEY;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SimpleKafkaSpecConsumer implements SpecConsumer<Spec>, Closeable {
  private static final String CONSUMER_CLIENT_FACTORY_CLASS_KEY = "spec.kafka.consumerClientClassFactory";
  private static final String DEFAULT_CONSUMER_CLIENT_FACTORY_CLASS =
      "org.apache.gobblin.kafka.client.Kafka08ConsumerClient$Factory";

  // Consumer
  protected final GobblinKafkaConsumerClient _kafkaConsumer;
  protected final List<KafkaPartition> _partitions;
  protected final List<Long> _lowWatermark;
  protected final List<Long> _nextWatermark;
  protected final List<Long> _highWatermark;

  private Iterator<KafkaConsumerRecord> messageIterator = null;
  private int currentPartitionIdx = -1;
  private boolean isFirstRun = true;

  private final BinaryDecoder _decoder;
  private final SpecificDatumReader<AvroJobSpec> _reader;
  private final SchemaVersionWriter<?> _versionWriter;

  public SimpleKafkaSpecConsumer(Config config, Optional<Logger> log) {

    // Consumer
    String kafkaConsumerClientClass = ConfigUtils.getString(config, CONSUMER_CLIENT_FACTORY_CLASS_KEY,
        DEFAULT_CONSUMER_CLIENT_FACTORY_CLASS);

    try {
      Class<?> clientFactoryClass = (Class<?>) Class.forName(kafkaConsumerClientClass);
      final GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory factory =
          (GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory)
              ConstructorUtils.invokeConstructor(clientFactoryClass);

      _kafkaConsumer = factory.create(config);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      if (log.isPresent()) {
        log.get().error("Failed to instantiate Kafka consumer from class " + kafkaConsumerClientClass, e);
      }

      throw new RuntimeException("Failed to instantiate Kafka consumer", e);
    }

    List<KafkaTopic> kafkaTopics = _kafkaConsumer.getFilteredTopics(Collections.EMPTY_LIST,
        Lists.newArrayList(Pattern.compile(config.getString(SimpleKafkaSpecExecutor.SPEC_KAFKA_TOPICS_KEY))));
    _partitions = kafkaTopics.get(0).getPartitions();
    _lowWatermark = Lists.newArrayList(Collections.nCopies(_partitions.size(), 0L));
    _nextWatermark = Lists.newArrayList(Collections.nCopies(_partitions.size(), 0L));
    _highWatermark = Lists.newArrayList(Collections.nCopies(_partitions.size(), 0L));

    InputStream dummyInputStream = new ByteArrayInputStream(new byte[0]);
    _decoder = DecoderFactory.get().binaryDecoder(dummyInputStream, null);
    _reader = new SpecificDatumReader<AvroJobSpec>(AvroJobSpec.SCHEMA$);
    _versionWriter = new FixedSchemaVersionWriter();
  }

  public SimpleKafkaSpecConsumer(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public SimpleKafkaSpecConsumer(Config config) {
    this(config, Optional.<Logger>absent());
  }

  @Override
  public Future<? extends List<Pair<SpecExecutor.Verb, Spec>>> changedSpecs() {
    List<Pair<SpecExecutor.Verb, Spec>> changesSpecs = new ArrayList<>();
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
          log.error(String.format("Failed to fetch next message buffer for partition %s. Will skip this partition.",
              getCurrentPartition()), e);
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
          final AvroJobSpec record;

          if (nextValidMessage instanceof ByteArrayBasedKafkaRecord) {
            record = decodeRecord((ByteArrayBasedKafkaRecord)nextValidMessage);
          } else if (nextValidMessage instanceof DecodeableKafkaRecord){
            record = ((DecodeableKafkaRecord<?, AvroJobSpec>) nextValidMessage).getValue();
          } else {
            throw new IllegalStateException(
                "Unsupported KafkaConsumerRecord type. The returned record can either be ByteArrayBasedKafkaRecord"
                    + " or DecodeableKafkaRecord");
          }

          JobSpec.Builder jobSpecBuilder = JobSpec.builder(record.getUri());

          Properties props = new Properties();
          props.putAll(record.getProperties());
          jobSpecBuilder.withJobCatalogURI(record.getUri()).withVersion(record.getVersion())
              .withDescription(record.getDescription()).withConfigAsProperties(props);

          if (!record.getTemplateUri().isEmpty()) {
            jobSpecBuilder.withTemplate(new URI(record.getTemplateUri()));
          }

          String verbName = record.getMetadata().get(VERB_KEY);
          SpecExecutor.Verb verb = SpecExecutor.Verb.valueOf(verbName);

          changesSpecs.add(new ImmutablePair<SpecExecutor.Verb, Spec>(verb, jobSpecBuilder.build()));
        } catch (Throwable t) {
          log.error("Could not decode record at partition " + this.currentPartitionIdx +
              " offset " + nextValidMessage.getOffset());
        }
      }
    }

    return new CompletedFuture(changesSpecs, null);
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
          long earliestOffset = _kafkaConsumer.getEarliestOffset(kafkaPartition);
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
        long latestOffset = _kafkaConsumer.getLatestOffset(kafkaPartition);
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
    return _kafkaConsumer.consume(_partitions.get(this.currentPartitionIdx),
        _nextWatermark.get(this.currentPartitionIdx), _highWatermark.get(this.currentPartitionIdx));
  }

  private AvroJobSpec decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) throws IOException {
    InputStream is = new ByteArrayInputStream(kafkaConsumerRecord.getMessageBytes());
    _versionWriter.readSchemaVersioningInformation(new DataInputStream(is));

    Decoder decoder = DecoderFactory.get().binaryDecoder(is, _decoder);

    return _reader.read(null, decoder);
  }

  @Override
  public void close() throws IOException {
    _kafkaConsumer.close();
  }
}