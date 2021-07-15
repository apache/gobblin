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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.TaskEventMetadataGenerator;
import org.apache.gobblin.util.TaskEventMetadataUtils;


/**
 * A class that tracks KafkaExtractor statistics such as record decode time, #processed records, #undecodeable records etc.
 *
 */
@Slf4j
public class KafkaExtractorStatsTracker {
  // Constants for event submission
  public static final String TOPIC = "topic";
  public static final String PARTITION = "partition";

  private static final String EMPTY_STRING = "";
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
  private static final String NULL_RECORD_COUNT = "nullRecordCount";
  private static final String PARTITION_TOTAL_SIZE = "partitionTotalSize";
  private static final String AVG_RECORD_PULL_TIME = "avgRecordPullTime";
  private static final String AVG_RECORD_SIZE = "avgRecordSize";
  private static final String READ_RECORD_TIME = "readRecordTime";
  private static final String DECODE_RECORD_TIME = "decodeRecordTime";
  private static final String FETCH_MESSAGE_BUFFER_TIME = "fetchMessageBufferTime";
  private static final String LAST_RECORD_HEADER_TIMESTAMP = "lastRecordHeaderTimestamp";
  private static final String OBSERVED_LATENCY_HISTOGRAM = "observedLatencyHistogram";

  @Getter
  private final Map<KafkaPartition, ExtractorStats> statsMap;
  private final Set<Integer> errorPartitions;
  private final WorkUnitState workUnitState;
  private final TaskEventMetadataGenerator taskEventMetadataGenerator;
  @Getter
  private final Histogram observedLatencyHistogram;
  private boolean isSlaConfigured;
  private long recordLevelSlaMillis;
  //Minimum partition index processed by this task. Statistics that are aggregated across all partitions (e.g. observed latency histogram)
  // processed by the task are reported against this partition index.
  private int minPartitionIdx = Integer.MAX_VALUE;

  //A global count of number of undecodeable messages encountered by the KafkaExtractor across all Kafka
  //TopicPartitions.
  @Getter
  private int undecodableMessageCount = 0;
  @Getter
  private int nullRecordCount = 0;

  private List<KafkaPartition> partitions;
  private long maxPossibleLatency;

  //Extractor stats aggregated across all partitions processed by the extractor.
  @Getter (AccessLevel.PACKAGE)
  @VisibleForTesting
  private AggregateExtractorStats aggregateExtractorStats = new AggregateExtractorStats();
  //Aggregate stats for the extractor derived from the most recently completed epoch
  private AggregateExtractorStats lastAggregateExtractorStats;

  public KafkaExtractorStatsTracker(WorkUnitState state, List<KafkaPartition> partitions) {
    this.workUnitState = state;
    this.partitions = partitions;
    this.statsMap = Maps.newHashMapWithExpectedSize(this.partitions.size());
    this.partitions.forEach(partition -> {
      this.statsMap.put(partition, new ExtractorStats());
      if (partition.getId() < minPartitionIdx) {
        minPartitionIdx = partition.getId();
      }
    });
    this.errorPartitions = Sets.newHashSet();
    if (this.workUnitState.contains(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY)) {
      this.isSlaConfigured = true;
      this.recordLevelSlaMillis = TimeUnit.MINUTES.toMillis(this.workUnitState.getPropAsLong(KafkaSource.RECORD_LEVEL_SLA_MINUTES_KEY));
    }
    this.taskEventMetadataGenerator = TaskEventMetadataUtils.getTaskEventMetadataGenerator(workUnitState);
    if (state.getPropAsBoolean(KafkaSource.OBSERVED_LATENCY_MEASUREMENT_ENABLED, KafkaSource.DEFAULT_OBSERVED_LATENCY_MEASUREMENT_ENABLED)) {
      this.observedLatencyHistogram = buildobservedLatencyHistogram(state);
    } else {
      this.observedLatencyHistogram = null;
    }
  }

  /**
   * A method that constructs a {@link Histogram} object based on a minimum value, a maximum value and precision in terms
   * of number of significant digits. The returned {@link Histogram} is not an auto-resizing histogram and any outliers
   * above the maximum possible value are discarded in favor of bounding the worst-case performance.
   *
   * @param state
   * @return a non auto-resizing {@link Histogram} with a bounded range and precision.
   */
  private Histogram buildobservedLatencyHistogram(WorkUnitState state) {
    this.maxPossibleLatency = TimeUnit.HOURS.toMillis(state.getPropAsInt(KafkaSource.MAX_POSSIBLE_OBSERVED_LATENCY_IN_HOURS,
        KafkaSource.DEFAULT_MAX_POSSIBLE_OBSERVED_LATENCY_IN_HOURS));
    int numSignificantDigits = state.getPropAsInt(KafkaSource.OBSERVED_LATENCY_PRECISION, KafkaSource.DEFAULT_OBSERVED_LATENCY_PRECISION);
    if (numSignificantDigits > 5) {
      log.warn("Max precision must be <= 5; Setting precision for observed latency to 5.");
      numSignificantDigits = 5;
    } else if (numSignificantDigits < 1) {
      log.warn("Max precision must be >= 1; Setting precision to the default value of 3.");
      numSignificantDigits = 3;
    }
    return new Histogram(1, maxPossibleLatency, numSignificantDigits);
  }

  public int getErrorPartitionCount() {
    return this.errorPartitions.size();
  }

  /**
   * A Java POJO that encapsulates per-partition extractor stats.
   */
  @Data
  public static class ExtractorStats {
    private long decodingErrorCount = -1L;
    private long nullRecordCount = -1L;
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
   * A Java POJO to track the aggregate extractor stats across all partitions processed by the extractor.
   */
  @Data
  public static class AggregateExtractorStats {
    private long maxIngestionLatency;
    private long numBytesConsumed;
    private long minStartFetchEpochTime = Long.MAX_VALUE;
    private long maxStopFetchEpochTime;
    private long minLogAppendTime = Long.MAX_VALUE;
    private long maxLogAppendTime;
    private long slaMissedRecordCount;
    private long processedRecordCount;
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
   *
   * @param partitionIdx index of Kafka topic partition.
   * @return the number of null valued records for a given partition id.
   */
  public Long getNullRecordCount(int partitionIdx) {
    return this.statsMap.get(this.partitions.get(partitionIdx)).getNullRecordCount();
  }

  /**
   * Called when the KafkaExtractor encounters an undecodeable record.
   */
  public void onUndecodeableRecord(int partitionIdx) {
    this.errorPartitions.add(partitionIdx);
    this.undecodableMessageCount++;
    incrementErrorCount(partitionIdx);
  }

  public void onNullRecord(int partitionIdx) {
    this.nullRecordCount++;
    incrementNullRecordCount(partitionIdx);
  }

  private void incrementNullRecordCount(int partitionIdx) {
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      if (v.nullRecordCount < 0) {
        v.nullRecordCount = 1;
      } else {
        v.nullRecordCount++;
      }
      return v;
    });
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
   * @param recordCreationTimestamp the time of the {@link org.apache.gobblin.kafka.client.KafkaConsumerRecord}.
   */
  public void onDecodeableRecord(int partitionIdx, long readStartTime, long decodeStartTime, long recordSizeInBytes, long logAppendTimestamp, long recordCreationTimestamp) {
    this.statsMap.computeIfPresent(this.partitions.get(partitionIdx), (k, v) -> {
      long currentTime = System.nanoTime();
      v.processedRecordCount++;
      v.partitionTotalSize += recordSizeInBytes;
      v.decodeRecordTime += currentTime - decodeStartTime;
      v.readRecordTime += currentTime - readStartTime;
      if (this.observedLatencyHistogram != null && recordCreationTimestamp > 0) {
        long observedLatency = System.currentTimeMillis() - recordCreationTimestamp;
        // Discard outliers larger than maxPossibleLatency to avoid additional overhead that may otherwise be incurred due to dynamic
        // re-sizing of Histogram when observedLatency exceeds the maximum assumed latency. Essentially, we trade-off accuracy for
        // performance in a pessimistic scenario.
        if (observedLatency < this.maxPossibleLatency) {
          this.observedLatencyHistogram.recordValue(observedLatency);
        }
      }
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
    updateAggregateExtractorStats(partitionIdx);
  }

  private void updateAggregateExtractorStats(int partitionIdx) {
    ExtractorStats partitionStats = this.statsMap.get(this.partitions.get(partitionIdx));

    if (partitionStats.getStartFetchEpochTime() < aggregateExtractorStats.getMinStartFetchEpochTime()) {
      aggregateExtractorStats.setMinStartFetchEpochTime(partitionStats.getStartFetchEpochTime());
    }
    if (partitionStats.getStopFetchEpochTime() > aggregateExtractorStats.getMaxStopFetchEpochTime()) {
      aggregateExtractorStats.setMaxStopFetchEpochTime(partitionStats.getStopFetchEpochTime());
    }

    long partitionLatency = 0L;
    //Check if there are any records consumed from this KafkaPartition.
    if (partitionStats.getMinLogAppendTime() > 0) {
      partitionLatency = partitionStats.getStopFetchEpochTime() - partitionStats.getMinLogAppendTime();
    }

    if (aggregateExtractorStats.getMaxIngestionLatency() < partitionLatency) {
      aggregateExtractorStats.setMaxIngestionLatency(partitionLatency);
    }

    if (aggregateExtractorStats.getMinLogAppendTime() > partitionStats.getMinLogAppendTime()) {
      aggregateExtractorStats.setMinLogAppendTime(partitionStats.getMinLogAppendTime());
    }

    if (aggregateExtractorStats.getMaxLogAppendTime() < partitionStats.getMaxLogAppendTime()) {
      aggregateExtractorStats.setMaxLogAppendTime(partitionStats.getMaxLogAppendTime());
    }

    aggregateExtractorStats.setProcessedRecordCount(aggregateExtractorStats.getProcessedRecordCount() + partitionStats.getProcessedRecordCount());
    aggregateExtractorStats.setNumBytesConsumed(aggregateExtractorStats.getNumBytesConsumed() + partitionStats.getPartitionTotalSize());

    if (partitionStats.getSlaMissedRecordCount() > 0) {
      aggregateExtractorStats.setSlaMissedRecordCount(aggregateExtractorStats.getSlaMissedRecordCount() + partitionStats.getSlaMissedRecordCount());
    }
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
    tagsForPartition.put(NULL_RECORD_COUNT, Long.toString(stats.getNullRecordCount()));
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

    //Report observed latency histogram as part
    if ((partitionId == minPartitionIdx) && (this.observedLatencyHistogram != null)) {
      tagsForPartition.put(OBSERVED_LATENCY_HISTOGRAM, convertHistogramToString(this.observedLatencyHistogram));
    }
    return tagsForPartition;
  }

  /**
   * A helper method to serialize a {@link Histogram} to its string representation. This method uses the
   * compressed logging format provided by the {@link org.HdrHistogram.HistogramLogWriter}
   * to represent the Histogram as a string. The readers can use the {@link org.HdrHistogram.HistogramLogReader} to
   * deserialize the string back to a {@link Histogram} object.
   * @param observedLatencyHistogram
   * @return
   */
  @VisibleForTesting
  public static String convertHistogramToString(Histogram observedLatencyHistogram) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (PrintStream stream = new PrintStream(baos, true, Charsets.UTF_8.name())) {
      HistogramLogWriter histogramLogWriter = new HistogramLogWriter(stream);
      histogramLogWriter.outputIntervalHistogram(observedLatencyHistogram);
      return new String(baos.toByteArray(), Charsets.UTF_8);
    } catch (UnsupportedEncodingException e) {
      log.error("Exception {} encountered when creating PrintStream; returning empty string", e);
      return EMPTY_STRING;
    }
  }

  /**
   * Emit Tracking events reporting the various statistics to be consumed by a monitoring application.
   * @param context the current {@link MetricContext}
   * @param tagsForPartitionsMap tags for each partition
   */
  public void emitTrackingEvents(MetricContext context, Map<KafkaPartition, Map<String, String>> tagsForPartitionsMap) {
    for (Map.Entry<KafkaPartition, Map<String, String>> eventTags : tagsForPartitionsMap.entrySet()) {
      EventSubmitter.Builder eventSubmitterBuilder = new EventSubmitter.Builder(context, GOBBLIN_KAFKA_NAMESPACE);
      eventSubmitterBuilder.addMetadata(this.taskEventMetadataGenerator.getMetadata(workUnitState, KAFKA_EXTRACTOR_TOPIC_METADATA_EVENT_NAME));
      eventSubmitterBuilder.build().submit(KAFKA_EXTRACTOR_TOPIC_METADATA_EVENT_NAME, eventTags.getValue());
    }
  }

  /**
   * A helper function to merge tags for KafkaPartition. Separate into a package-private method for ease of testing.
   */
  public Map<KafkaPartition, Map<String, String>> generateTagsForPartitions(MultiLongWatermark lowWatermark, MultiLongWatermark highWatermark,
      MultiLongWatermark nextWatermark, Map<KafkaPartition, Map<String, String>> additionalTags) {
    Map<KafkaPartition, Map<String, String>> tagsForPartitionsMap = Maps.newHashMap();
    for (int i = 0; i < this.partitions.size(); i++) {
      KafkaPartition partitionKey = this.partitions.get(i);

      log.info(String.format("Actual high watermark for partition %s=%d, expected=%d", this.partitions.get(i),
          nextWatermark.get(i), highWatermark.get(i)));
      tagsForPartitionsMap
          .put(this.partitions.get(i), createTagsForPartition(i, lowWatermark, highWatermark, nextWatermark));

      // Merge with additionalTags from argument-provided map if exists.
      if (additionalTags.containsKey(partitionKey)) {
        tagsForPartitionsMap.get(partitionKey).putAll(additionalTags.get(partitionKey));
      }
    }

    return tagsForPartitionsMap;
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
   * @param timeUnit the time unit for the ingestion latency.
   * @return the maximum ingestion latency across all partitions processed by the extractor from the last
   * completed epoch.
   */
  public long getMaxIngestionLatency(TimeUnit timeUnit) {
    return timeUnit.convert(this.lastAggregateExtractorStats.getMaxIngestionLatency(), TimeUnit.MILLISECONDS);
  }

  /**
   *
   * @return the consumption rate in MB/s across all partitions processed by the extractor from the last
   * completed epoch.
   */
  public double getConsumptionRateMBps() {
    double consumptionDurationSecs = ((double) (this.lastAggregateExtractorStats.getMaxStopFetchEpochTime() - this.lastAggregateExtractorStats
            .getMinStartFetchEpochTime())) / 1000;
    return this.lastAggregateExtractorStats.getNumBytesConsumed() / (consumptionDurationSecs * (1024 * 1024L));
  }

  /**
   * Reset all KafkaExtractor stats.
   */
  public void reset() {
    this.lastAggregateExtractorStats = this.aggregateExtractorStats;
    this.aggregateExtractorStats = new AggregateExtractorStats();
    this.partitions.forEach(partition -> this.statsMap.put(partition, new ExtractorStats()));
    for (int partitionIdx = 0; partitionIdx < this.partitions.size(); partitionIdx++) {
      resetStartFetchEpochTime(partitionIdx);
    }
    if (this.observedLatencyHistogram != null) {
      this.observedLatencyHistogram.reset();
    }
  }
}
