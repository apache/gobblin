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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;


/**
 * A class that tracks KafkaExtractor statistics such as record decode time, #processed records, #undecodeable records etc.
 *
 */
@Slf4j
public class KafkaExtractorStatsTracker {
  // Constants for event submission
  public static final String TOPIC = "topic";
  public static final String PARTITION = "partition";

  private static final String GOBBLIN_KAFKA_NAMESPACE = "gobblin.kafka";
  private static final String KAFKA_EXTRACTOR_TOPIC_METADATA_EVENT_NAME = "KafkaExtractorTopicMetadata";
  private static final String LOW_WATERMARK = "lowWatermark";
  private static final String ACTUAL_HIGH_WATERMARK = "actualHighWatermark";
  private static final String EXPECTED_HIGH_WATERMARK = "expectedHighWatermark";
  private static final String ELAPSED_TIME = "elapsedTime";
  private static final String PROCESSED_RECORD_COUNT = "processedRecordCount";
  private static final String SLA_MISSED_RECORD_COUNT = "slaMissedRecordCount";
  private static final String MIN_LOG_APPEND_TIMESTAMP = "minLogAppendTimestamp";
  private static final String MAX_LOG_APPEND_TIMESTAMP = "maxLogAppendTimestamp";
  private static final String UNDECODABLE_MESSAGE_COUNT = "undecodableMessageCount";
  private static final String PARTITION_TOTAL_SIZE = "partitionTotalSize";
  private static final String AVG_RECORD_PULL_TIME = "avgRecordPullTime";
  private static final String AVG_RECORD_SIZE = "avgRecordSize";
  private static final String READ_RECORD_TIME = "readRecordTime";
  private static final String DECODE_RECORD_TIME = "decodeRecordTime";
  private static final String FETCH_MESSAGE_BUFFER_TIME = "fetchMessageBufferTime";
  private static final String LAST_RECORD_HEADER_TIMESTAMP = "lastRecordHeaderTimestamp";

  @Getter
  private final Map<KafkaPartition, ExtractorStats> statsMap;
  private final Set<Integer> errorPartitions;
  private final WorkUnitState workUnitState;
  private boolean isSlaConfigured;
  private long recordLevelSlaMillis;

  //A global count of number of undecodeable messages encountered by the KafkaExtractor across all Kafka
  //TopicPartitions.
  @Getter
  private int undecodableMessageCount = 0;
  private List<KafkaPartition> partitions;

  public KafkaExtractorStatsTracker(WorkUnitState state, List<KafkaPartition> partitions) {
    this.workUnitState = state;
    this.partitions = partitions;
    this.statsMap = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.partitions.forEach(partition -> this.statsMap.put(partition, new ExtractorStats()));
    this.errorPartitions = Sets.newHashSet();
    if (this.workUnitState.contains(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY)) {
      this.isSlaConfigured = true;
      this.recordLevelSlaMillis = TimeUnit.MINUTES.toMillis(this.workUnitState.getPropAsLong(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY));
    }
  }

  public int getErrorPartitionCount() {
    return this.errorPartitions.size();
  }

  /**
   * A Java POJO that encapsulates various extractor stats.
   */
  @Data
  public static class ExtractorStats {
    private long decodingErrorCount = -1L;
    private double avgMillisPerRecord = -1;
    private long avgRecordSize;
    private long elapsedTime;
    private long processedRecordCount;
    private long slaMissedRecordCount = -1L;
    private long partitionTotalSize;
    private long decodeRecordTime;
    private long fetchMessageBufferTime;
    private long readRecordTime;
    private long startFetchEpochTime;
    private long stopFetchEpochTime;
    private long lastSuccessfulRecordHeaderTimestamp;
    private long minLogAppendTime = -1L;
    private long maxLogAppendTime = -1L;
  }

  /**
   *
   * @param partitionIdx index of Kafka topic partition.
   * @return the number of undecodeable records for a given partition id.
   */
  public Long getDecodingErrorCount(int partitionIdx) {
    return this.statsMap.get(this.partitions.get(partitionIdx)).getDecodingErrorCount();
  }

  /**
   * Called when the KafkaExtractor encounters an undecodeable record.
   */
  public void onUndecodeableRecord(int partitionIdx) {
    this.errorPartitions.add(partitionIdx);
    this.undecodableMessageCount++;
    incrementErrorCount(partitionIdx);
  }

  private void incrementErrorCount(int partitionIdx) {
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      if (v.decodingErrorCount < 0) {
        v.decodingErrorCount = 1;
      } else {
        v.decodingErrorCount++;
      }
      return v;
    });
  }

  public void resetStartFetchEpochTime(int partitionIdx) {
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      v.startFetchEpochTime = System.currentTimeMillis();
      return v;
    });
  }

  /**
   * A method that is called when a Kafka record is successfully decoded.
   * @param partitionIdx the index of Kafka Partition .
   * @param readStartTime the start time when readRecord() is invoked.
   * @param decodeStartTime the time instant immediately before a record decoding begins.
   * @param recordSizeInBytes the size of the decoded record in bytes.
   * @param logAppendTimestamp the log append time of the {@link org.apache.gobblin.kafka.client.KafkaConsumerRecord}.
   */
  public void onDecodeableRecord(int partitionIdx, long readStartTime, long decodeStartTime, long recordSizeInBytes, long logAppendTimestamp) {
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      long currentTime = System.nanoTime();
      v.processedRecordCount++;
      v.partitionTotalSize += recordSizeInBytes;
      v.decodeRecordTime += currentTime - decodeStartTime;
      v.readRecordTime += currentTime - readStartTime;
      if (this.isSlaConfigured) {
        if (v.slaMissedRecordCount < 0) {
          v.slaMissedRecordCount = 0;
          v.minLogAppendTime = logAppendTimestamp;
          v.maxLogAppendTime = logAppendTimestamp;
        } else {
          if (logAppendTimestamp < v.minLogAppendTime) {
            v.minLogAppendTime = logAppendTimestamp;
          }
          if (logAppendTimestamp > v.maxLogAppendTime) {
            v.maxLogAppendTime = logAppendTimestamp;
          }
        }
        if (logAppendTimestamp > 0 && (System.currentTimeMillis() - logAppendTimestamp > recordLevelSlaMillis)) {
          v.slaMissedRecordCount++;
        }
      }
      return v;
    });
  }

  /**
   * A method that is called after a batch of records has been fetched from Kafka e.g. via a consumer.poll().
   * @param partitionIdx the index of Kafka partition
   * @param fetchStartTime the time instant immediately before fetching records from Kafka.
   */
  public void onFetchNextMessageBuffer(int partitionIdx, long fetchStartTime) {
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      v.fetchMessageBufferTime += System.nanoTime() - fetchStartTime;
      return v;
    });
  }

  /**
   * A method when a partition has been processed.
   * @param partitionIdx the index of Kafka partition
   * @param readStartTime the start time when readRecord.
   */
  void onPartitionReadComplete(int partitionIdx, long readStartTime) {
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      v.readRecordTime += System.nanoTime() - readStartTime;
      return v;
    });
  }

  /**
   * A method that is invoked to update the statistics for current partition. In the batch mode of execution, this is
   * invoked when a partition has been processed and before the next partition can be processed. In the streaming mode of
   * execution, this method is invoked on every flush.
   * @param partitionIdx the index of Kafka partition
   * @param readStartTime the start time when readRecord() is invoked.
   */
  public void updateStatisticsForCurrentPartition(int partitionIdx, long readStartTime, long lastSuccessfulRecordHeaderTimestamp) {
    long stopFetchEpochTime = System.currentTimeMillis();
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      v.stopFetchEpochTime = stopFetchEpochTime;
      if (v.processedRecordCount != 0) {
        v.elapsedTime = stopFetchEpochTime - this.statsMap.get(this.partitions.get(partitionIdx)).getStartFetchEpochTime();
        //Compute average stats
        v.avgMillisPerRecord = (double) v.elapsedTime / (double) v.processedRecordCount;
        v.avgRecordSize = this.statsMap.get(this.partitions.get(partitionIdx)).getPartitionTotalSize() / v.processedRecordCount;
        v.lastSuccessfulRecordHeaderTimestamp = lastSuccessfulRecordHeaderTimestamp;
      }
      return v;
    });
    onPartitionReadComplete(partitionIdx, readStartTime);
  }

  private Map<String, String> createTagsForPartition(int partitionId, MultiLongWatermark lowWatermark, MultiLongWatermark highWatermark, MultiLongWatermark nextWatermark) {
    Map<String, String> tagsForPartition = Maps.newHashMap();
    KafkaPartition partition = this.partitions.get(partitionId);

    tagsForPartition.put(TOPIC, partition.getTopicName());
    tagsForPartition.put(PARTITION, Integer.toString(partition.getId()));
    tagsForPartition.put(LOW_WATERMARK, Long.toString(lowWatermark.get(partitionId)));
    tagsForPartition.put(ACTUAL_HIGH_WATERMARK, Long.toString(nextWatermark.get(partitionId)));

    // These are used to compute the load factor,
    // gobblin consumption rate relative to the kafka production rate.
    // The gobblin rate is computed as (processed record count/elapsed time)
    // The kafka rate is computed as (expected high watermark - previous latest offset) /
    // (current offset fetch epoch time - previous offset fetch epoch time).
    tagsForPartition.put(EXPECTED_HIGH_WATERMARK, Long.toString(highWatermark.get(partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_OFFSET_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_OFFSET_FETCH_EPOCH_TIME, partitionId)));
    tagsForPartition.put(KafkaSource.OFFSET_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.OFFSET_FETCH_EPOCH_TIME, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_LATEST_OFFSET,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_LATEST_OFFSET, partitionId)));

    tagsForPartition.put(KafkaSource.PREVIOUS_LOW_WATERMARK,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_LOW_WATERMARK, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_HIGH_WATERMARK,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_HIGH_WATERMARK, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_START_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_START_FETCH_EPOCH_TIME, partitionId)));
    tagsForPartition.put(KafkaSource.PREVIOUS_STOP_FETCH_EPOCH_TIME,
        Long.toString(KafkaUtils.getPropAsLongFromSingleOrMultiWorkUnitState(this.workUnitState,
            KafkaSource.PREVIOUS_STOP_FETCH_EPOCH_TIME, partitionId)));

    ExtractorStats stats = this.statsMap.getOrDefault(partition, new ExtractorStats());

    tagsForPartition.put(KafkaSource.START_FETCH_EPOCH_TIME, Long.toString(stats.getStartFetchEpochTime()));
    tagsForPartition.put(KafkaSource.STOP_FETCH_EPOCH_TIME, Long.toString(stats.getStopFetchEpochTime()));
    this.workUnitState.setProp(KafkaUtils.getPartitionPropName(KafkaSource.START_FETCH_EPOCH_TIME, partitionId),
        Long.toString(stats.getStartFetchEpochTime()));
    this.workUnitState.setProp(KafkaUtils.getPartitionPropName(KafkaSource.STOP_FETCH_EPOCH_TIME, partitionId),
        Long.toString(stats.getStopFetchEpochTime()));
    tagsForPartition.put(PROCESSED_RECORD_COUNT, Long.toString(stats.getProcessedRecordCount()));
    tagsForPartition.put(SLA_MISSED_RECORD_COUNT, Long.toString(stats.getSlaMissedRecordCount()));
    tagsForPartition.put(MIN_LOG_APPEND_TIMESTAMP, Long.toString(stats.getMinLogAppendTime()));
    tagsForPartition.put(MAX_LOG_APPEND_TIMESTAMP, Long.toString(stats.getMaxLogAppendTime()));
    tagsForPartition.put(PARTITION_TOTAL_SIZE, Long.toString(stats.getPartitionTotalSize()));
    tagsForPartition.put(AVG_RECORD_SIZE, Long.toString(stats.getAvgRecordSize()));
    tagsForPartition.put(ELAPSED_TIME, Long.toString(stats.getElapsedTime()));
    tagsForPartition.put(DECODE_RECORD_TIME, Long.toString(TimeUnit.NANOSECONDS.toMillis(stats.getDecodeRecordTime())));
    tagsForPartition.put(FETCH_MESSAGE_BUFFER_TIME,
        Long.toString(TimeUnit.NANOSECONDS.toMillis(stats.getFetchMessageBufferTime())));
    tagsForPartition.put(READ_RECORD_TIME, Long.toString(TimeUnit.NANOSECONDS.toMillis(stats.getReadRecordTime())));
    tagsForPartition.put(UNDECODABLE_MESSAGE_COUNT, Long.toString(stats.getDecodingErrorCount()));
    tagsForPartition.put(LAST_RECORD_HEADER_TIMESTAMP, Long.toString(stats.getLastSuccessfulRecordHeaderTimestamp()));

    // Commit avg time to pull a record for each partition
    double avgMillis = stats.getAvgMillisPerRecord();
    if (avgMillis >= 0) {
      log.info(String.format("Avg time to pull a record for partition %s = %f milliseconds", partition, avgMillis));
      KafkaUtils.setPartitionAvgRecordMillis(this.workUnitState, partition, avgMillis);
      tagsForPartition.put(AVG_RECORD_PULL_TIME, Double.toString(avgMillis));
    } else {
      log.info(String.format("Avg time to pull a record for partition %s not recorded", partition));
      tagsForPartition.put(AVG_RECORD_PULL_TIME, Double.toString(-1));
    }

    return tagsForPartition;
  }

  /**
   * Emit Tracking events reporting the various statistics to be consumed by a monitoring application.
   * @param context the current {@link MetricContext}
   * @param lowWatermark begin Kafka offset for each topic partition
   * @param highWatermark the expected last Kafka offset for each topic partition to be consumed by the Extractor
   * @param nextWatermark the offset of next valid message for each Kafka topic partition consumed by the Extractor
   */
  public void emitTrackingEvents(MetricContext context, MultiLongWatermark lowWatermark, MultiLongWatermark highWatermark,
      MultiLongWatermark nextWatermark) {
    Map<KafkaPartition, Map<String, String>> tagsForPartitionsMap = Maps.newHashMap();
    for (int i = 0; i < this.partitions.size(); i++) {
      log.info(String.format("Actual high watermark for partition %s=%d, expected=%d", this.partitions.get(i),
          nextWatermark.get(i), highWatermark.get(i)));
      tagsForPartitionsMap
          .put(this.partitions.get(i), createTagsForPartition(i, lowWatermark, highWatermark, nextWatermark));
    }
    for (Map.Entry<KafkaPartition, Map<String, String>> eventTags : tagsForPartitionsMap.entrySet()) {
      new EventSubmitter.Builder(context, GOBBLIN_KAFKA_NAMESPACE).build()
          .submit(KAFKA_EXTRACTOR_TOPIC_METADATA_EVENT_NAME, eventTags.getValue());
    }
  }

  /**
   *
   * @param partitionIdx the index of Kafka partition
   * @return the average record size of records for a given {@link KafkaPartition}
   */
  public long getAvgRecordSize(int partitionIdx) {
    ExtractorStats stats = this.statsMap.getOrDefault(this.partitions.get(partitionIdx), null);
    if (stats != null) {
      if (stats.getAvgRecordSize() != 0) {
        //Average record size already computed.
        return stats.getAvgRecordSize();
      } else {
        //Compute average record size
        if (stats.getProcessedRecordCount() != 0) {
          return stats.getPartitionTotalSize() / stats.getProcessedRecordCount();
        }
      }
    }
    return 0;
  }

  /**
   * Reset all KafkaExtractor stats.
   */
  public void reset() {
    this.partitions.forEach(partition -> this.statsMap.put(partition, new ExtractorStats()));
    for (int partitionIdx = 0; partitionIdx < this.partitions.size(); partitionIdx++) {
      resetStartFetchEpochTime(partitionIdx);
    }
  }
}
