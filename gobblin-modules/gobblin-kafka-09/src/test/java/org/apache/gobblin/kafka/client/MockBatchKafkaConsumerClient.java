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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.api.client.util.Lists;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;


/**
 * A Mock Kafka Consumer client that returns an (@link #MockBatchIterator} for a {@link #iteratorBatchSize}
 * each time consume called.
 * Returns empty iterator if all {@link #records} have been consumed.
 */
public class MockBatchKafkaConsumerClient extends AbstractBaseKafkaConsumerClient {

  private final Map<KafkaPartition, Long> latestOffsets;
  private final List<KafkaConsumerRecord> records;
  private int currRecordIdx = 0;
  private final int iteratorBatchSize;

  public MockBatchKafkaConsumerClient(Config config, List<KafkaConsumerRecord> records, int batchSize) {
    super(config);
    this.latestOffsets = Maps.newHashMap();
    this.records = records;
    this.iteratorBatchSize = batchSize;
  }

  @Override
  public List<KafkaTopic> getTopics() {
    return Lists.newArrayList();
  }

  @Override
  public long getEarliestOffset(KafkaPartition partition)
      throws KafkaOffsetRetrievalFailureException {
    return 0;
  }

  @Override
  public long getLatestOffset(KafkaPartition partition)
      throws KafkaOffsetRetrievalFailureException {
    return 0;
  }

  @Override
  public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {
    return null;
  }

  @Override
  public Iterator<KafkaConsumerRecord> consume() {
    return consume(iteratorBatchSize);
  }

  private Iterator<KafkaConsumerRecord> consume(int batchSize) {
    if (currRecordIdx < this.records.size() && batchSize > 0) {
      Iterator iterator = new MockBatchIterator(batchSize, records, currRecordIdx);
      currRecordIdx+=iteratorBatchSize;
      return iterator;
    }
    return Iterators.emptyIterator();
  }

  public void commitOffsets(Map<KafkaPartition, Long> partitionOffsets) {
    latestOffsets.putAll(partitionOffsets);
  }

  public Map<KafkaPartition, Long> getCommittedOffsets() {
    return latestOffsets;
  }

  public void addToRecords(KafkaConsumerRecord record) {
    this.records.add(record);
  }

  @Override
  public void close()
      throws IOException {

  }

  /**
   * Iterator that can iterate over a range of {@link #records}
   * should be instantiated each time for a given range
   */
  class MockBatchIterator implements Iterator<KafkaConsumerRecord> {

    private final List<KafkaConsumerRecord> records;
    private int currIdx;
    private int endIdx;

    public MockBatchIterator(int batchSize, List<KafkaConsumerRecord> records, int currIdx) {
      this.records = records;
      this.currIdx = currIdx;
      this.endIdx = currIdx + batchSize - 1;
    }

    @Override
    public boolean hasNext() {
      return (this.currIdx <= this.endIdx && this.currIdx < this.records.size());
    }

    @Override
    public KafkaConsumerRecord next() {
      KafkaConsumerRecord record = records.get(currIdx);
      this.currIdx++;
      return record;
    }
  }
}
