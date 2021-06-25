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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;

import com.codahale.metrics.Metric;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.util.ConfigUtils;

import static org.mockito.Mockito.mock;


/**
 * A bunch of basic mocking class that can be used for different implementation to mock kafka clients.
 */
public class KafkaStreamTestUtils {
  public static class MockKafkaConsumerClientFactory
      implements GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory {
    static final AbstractBaseKafkaConsumerClient MOCKED_KAFKA_CLIENT = mock(AbstractBaseKafkaConsumerClient.class);

    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      return MOCKED_KAFKA_CLIENT;
    }
  }

  public static class MockKafka10ConsumerClientFactory
      implements GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory {
    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      return new MockKafkaConsumerClient(config);
    }
  }

  /**
   * A mock implementation of {@link GobblinKafkaConsumerClient} that returns a {@link MockIterator} on
   * invocation of the {@link MockKafkaConsumerClient#consume()} method.
   */
  public static class MockKafkaConsumerClient implements GobblinKafkaConsumerClient {
    public static final String NUM_PARTITIONS_ASSIGNED = "gobblin.kafka.streaming.numPartitions";
    public static final String CAN_RETURN_NULL_VALUED_RECORDS = "gobblin.kafka.streaming.canReturnNulls";

    private final Map<KafkaPartition, Long> latestOffsets = Maps.newHashMap();
    private final Random random = new Random();
    private final String topicName;
    private final boolean canReturnNullValuedRecords;
    private final List<Integer> partitionIds;

    protected MockKafkaConsumerClient(Config baseConfig) {
      this.topicName = baseConfig.getString(KafkaSource.TOPIC_NAME);
      int numPartitionsAssigned = ConfigUtils.getInt(baseConfig, NUM_PARTITIONS_ASSIGNED, 0);
      this.canReturnNullValuedRecords = ConfigUtils.getBoolean(baseConfig, CAN_RETURN_NULL_VALUED_RECORDS, false);
      this.partitionIds = getPartitionIds(baseConfig, numPartitionsAssigned);
    }

    private List<Integer> getPartitionIds(Config baseConfig, int numPartitionsAssigned) {
      List<Integer> partitionIds = Lists.newArrayList();
      for (int i = 0; i < numPartitionsAssigned; i++) {
        String partitionIdProp = KafkaSource.PARTITION_ID + "." + i;
        partitionIds.add(baseConfig.getInt(partitionIdProp));
      }
      return partitionIds;
    }

    /**
     *
     * @return a {@link MockIterator} over {@link KafkaConsumerRecord}s.
     */
    @Override
    public Iterator<KafkaConsumerRecord> consume() {
      return new MockIterator(this.topicName, this.partitionIds, this.canReturnNullValuedRecords);
    }

    @Override
    public void assignAndSeek(List<KafkaPartition> topicPartitions,
        Map<KafkaPartition, LongWatermark> topicWatermarks) {
      return;
    }

    @Override
    public List<KafkaTopic> getFilteredTopics(List<Pattern> blacklist, List<Pattern> whitelist) {
      return null;
    }

    @Override
    public long getEarliestOffset(KafkaPartition partition) {
      return 0;
    }

    @Override
    public long getLatestOffset(KafkaPartition partition) {
      return 0;
    }

    /**
     * Returns a random offset for each {@link KafkaPartition}. The method ensures that the offsets are monotonically
     * increasing for each {@link KafkaPartition} i.e. each subsequent call to the method will return a higher offset
     * for every partition in the partition list.
     * @param partitions
     * @return
     */
    @Override
    public Map<KafkaPartition, Long> getLatestOffsets(Collection<KafkaPartition> partitions) {
      for (KafkaPartition partition : partitions) {
        if (this.latestOffsets.containsKey(partition)) {
          this.latestOffsets.put(partition, this.latestOffsets.get(partition) + 100);
        } else {
          this.latestOffsets.put(partition, new Long(random.nextInt(100000)));
        }
      }
      return this.latestOffsets;
    }

    @Override
    public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {
      return null;
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return new HashMap<>();
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  public static class MockSchemaRegistry extends KafkaSchemaRegistry<String, Schema> {
    static Schema latestSchema = Schema.create(Schema.Type.STRING);

    public MockSchemaRegistry(Properties props) {
      super(props);
    }

    @Override
    protected Schema fetchSchemaByKey(String key) {
      return null;
    }

    @Override
    public Schema getLatestSchemaByTopic(String topic) {
      return latestSchema;
    }

    @Override
    public String register(Schema schema) {
      return null;
    }

    @Override
    public String register(Schema schema, String name) {
      latestSchema = schema;
      return schema.toString();
    }
  }

  public static class LowLevelMockSchemaRegistry
      implements org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistry<String, Schema> {
    private Schema latestSchema;

    public LowLevelMockSchemaRegistry(Properties props) {
    }

    @Override
    public String register(String name, Schema schema) {
      this.latestSchema = schema;
      return schema.toString();
    }

    @Override
    public Schema getById(String id) {
      return null;
    }

    @Override
    public Schema getLatestSchema(String name) {
      return this.latestSchema;
    }

    @Override
    public boolean hasInternalCache() {
      return false;
    }
  }

  /**
   * A mock iterator of {@link KafkaConsumerRecord}s. The caller provides a topicName, a list of partition ids and
   * optionally, the number of records the iterator must iterate over. On each call to next(), the iterator returns
   * a mock {@link KafkaConsumerRecord}, with a partition id assigned in a round-robin fashion over the input list of
   * partition ids.
   */
  public static class MockIterator implements Iterator<KafkaConsumerRecord> {
    // Schema for LiKafka10ConsumerRecords. TODO: Enhance the iterator to return random records
    // according to a given schema.
    private static final String SCHEMA =
        "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"user\",\n"
            + " \"fields\": [\n" + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"DUMMY\", \"type\": [\"null\",\"string\"]}\n" + " ]\n" + "}";

    private final Schema schema = new Schema.Parser().parse(SCHEMA);
    private final String topicName;
    private final long maxNumRecords;
    private final List<Integer> partitionIds;
    private final long[] nextOffsets;
    private final boolean canReturnNullRecords;
    private long numRecordsReturnedSoFar;
    private int partitionIdx = 0;

    public MockIterator(String topicName, List<Integer> partitionIds, boolean canReturnNullRecords) {
      this(topicName, partitionIds, canReturnNullRecords, Long.MAX_VALUE);
    }

    public MockIterator(String topicName, List<Integer> partitionIds, boolean canReturnNullRecords, long numRecords) {
      this.topicName = topicName;
      this.maxNumRecords = numRecords;
      this.partitionIds = partitionIds;
      this.canReturnNullRecords = canReturnNullRecords;
      this.nextOffsets = new long[partitionIds.size()];
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
      return this.numRecordsReturnedSoFar < this.maxNumRecords;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws java.util.NoSuchElementException if the iteration has no more elements
     */
    @Override
    public KafkaConsumerRecord next() {
      this.numRecordsReturnedSoFar++;
      return getMockConsumerRecord();
    }

    private KafkaConsumerRecord getMockConsumerRecord() {
      DecodeableKafkaRecord mockRecord = Mockito.mock(DecodeableKafkaRecord.class);
      Mockito.when(mockRecord.getValue()).thenReturn(getRecord());
      Mockito.when(mockRecord.getTopic()).thenReturn(topicName);
      Mockito.when(mockRecord.getPartition()).thenReturn(this.partitionIds.get(partitionIdx));
      this.partitionIdx = (this.partitionIdx + 1) % this.partitionIds.size();
      //Increment the next offset of the record
      this.nextOffsets[partitionIdx]++;
      Mockito.when(mockRecord.getNextOffset()).thenReturn(this.nextOffsets[partitionIdx]);
      return mockRecord;
    }

    private GenericRecord getRecord() {
      if ((!this.canReturnNullRecords) || (this.numRecordsReturnedSoFar % 2 == 0)) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", UUID.randomUUID());
        return record;
      } else {
        return null;
      }
    }
  }
}

