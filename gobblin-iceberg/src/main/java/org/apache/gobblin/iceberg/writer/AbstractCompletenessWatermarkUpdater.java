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

package org.apache.gobblin.iceberg.writer;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.completeness.verifier.KafkaAuditCountVerifier;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.time.TimeIterator;

import static org.apache.gobblin.iceberg.writer.IcebergMetadataWriterConfigKeys.*;

/**
 * An abstract class for completeness watermark updater.
 * It computes the new watermark and updates below entities:
 *  1. the properties in {@link IcebergMetadataWriter.TableMetadata}
 *  2. {@link gobblin.configuration.State}
 *  3. the completionWatermark in {@link IcebergMetadataWriter.TableMetadata}
 */
@Slf4j
public abstract class AbstractCompletenessWatermarkUpdater {
  private final String topic;
  private final String auditCheckGranularity;

  protected final String timeZone;
  protected final IcebergMetadataWriter.TableMetadata tableMetadata;
  protected final Map<String, String> propsToUpdate;
  protected final State stateToUpdate;
  protected final KafkaAuditCountVerifier auditCountVerifier;

  public AbstractCompletenessWatermarkUpdater(String topic, String auditCheckGranularity, String timeZone,
      IcebergMetadataWriter.TableMetadata tableMetadata, Map<String, String> propsToUpdate, State stateToUpdate,
      KafkaAuditCountVerifier auditCountVerifier) {
    this.tableMetadata = tableMetadata;
    this.topic = topic;
    this.auditCheckGranularity = auditCheckGranularity;
    this.timeZone = timeZone;
    this.propsToUpdate = propsToUpdate;
    this.stateToUpdate = stateToUpdate;
    this.auditCountVerifier = auditCountVerifier;
  }

  /**
   * Update TableMetadata with the new completion watermark upon a successful audit check
   * @param timestamps Sorted set in reverse order of timestamps to check audit counts for
   */
  public void run(SortedSet<ZonedDateTime> timestamps) {
    String tableName = tableMetadata.table.get().name();
    if (this.topic == null) {
      log.error(String.format("Not performing audit check. %s is null. Please set as table property of %s",
          TOPIC_NAME_KEY, tableName));
    }
    long previousCompletenessWatermark = getCompletionWatermarkFromTableMetadata();
    long newCompletenessWatermark = computeWatermark(tableName, previousCompletenessWatermark, timestamps);
    if (newCompletenessWatermark > previousCompletenessWatermark) {
      log.info(String.format("Updating %s for %s to %s", COMPLETION_WATERMARK_KEY, this.tableMetadata.table.get().name(),
          newCompletenessWatermark));
      updateProps(newCompletenessWatermark);
      updateCompletionWatermarkInTableMetadata(newCompletenessWatermark);
    }
  }

  abstract boolean checkCompleteness(String datasetName, long beginInMillis, long endInMillis) throws IOException;
  abstract void updateProps(long newCompletenessWatermark);
  abstract void updateState(String catalogDbTableNameLowerCased, long newCompletenessWatermark);
  abstract void updateCompletionWatermarkInTableMetadata(long newCompletenessWatermark);
  abstract long getCompletionWatermarkFromTableMetadata();

  /**
   * NOTE: completion watermark for a window [t1, t2] is marked as t2 if audit counts match
   * for that window (aka its is set to the beginning of next window)
   * For each timestamp in sorted collection of timestamps in descending order
   * if timestamp is greater than previousWatermark
   * and hour(now) > hour(prevWatermark)
   *    check audit counts for completeness between
   *    a source and reference tier for [timestamp -1 , timstamp unit of granularity]
   *    If the audit count matches update the watermark to the timestamp and break
   *    else continue
   * else
   *  break
   * Using a {@link TimeIterator} that operates over a range of time in 1 unit
   * given the start, end and granularity
   * @param catalogDbTableName
   * @param previousWatermark previous completion watermark for the table
   * @param timestamps Sorted set in reverse order of timestamps to check audit counts for
   * @return updated completion watermark
   */
  private long computeWatermark(String catalogDbTableName, long previousWatermark, SortedSet<ZonedDateTime> timestamps) {
    log.info(String.format("Compute completion watermark for %s and timestamps %s with previous watermark %s", topic,
        timestamps, previousWatermark));
    long completionWatermark = previousWatermark;
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of(this.timeZone));
    try {
      if(timestamps == null || timestamps.size() <= 0) {
        log.error("Cannot create time iterator. Empty for null timestamps");
        return previousWatermark;
      }
      TimeIterator.Granularity granularity = TimeIterator.Granularity.valueOf(this.auditCheckGranularity);
      ZonedDateTime prevWatermarkDT = Instant.ofEpochMilli(previousWatermark)
          .atZone(ZoneId.of(this.timeZone));
      ZonedDateTime startDT = timestamps.first();
      ZonedDateTime endDT = timestamps.last();
      TimeIterator iterator = new TimeIterator(startDT, endDT, granularity, true);
      while (iterator.hasNext()) {
        ZonedDateTime timestampDT = iterator.next();
        if (timestampDT.isAfter(prevWatermarkDT)
            && TimeIterator.durationBetween(prevWatermarkDT, now, granularity) > 0) {
          long timestampMillis = timestampDT.toInstant().toEpochMilli();
          ZonedDateTime auditCountCheckLowerBoundDT = TimeIterator.dec(timestampDT, granularity, 1);
          boolean complete = checkCompleteness(this.topic, auditCountCheckLowerBoundDT.toInstant().toEpochMilli(),
              timestampMillis);
          if (complete) {
            completionWatermark = timestampMillis;
            // Also persist the watermark into State object to share this with other MetadataWriters
            // we enforce ourselves to always use lower-cased table name here
            String catalogDbTableNameLowerCased = catalogDbTableName.toLowerCase(Locale.ROOT);
            updateState(catalogDbTableNameLowerCased, completionWatermark);
            break;
          }
        } else {
          break;
        }
      }
    } catch (IOException e) {
      log.warn("Exception during audit count check: ", e);
    }
    return completionWatermark;
  }
}
