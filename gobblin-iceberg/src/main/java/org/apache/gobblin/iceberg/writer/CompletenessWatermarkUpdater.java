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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.completeness.verifier.KafkaAuditCountVerifier;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.time.TimeIterator;

import static org.apache.gobblin.iceberg.writer.IcebergMetadataWriterConfigKeys.*;

/**
 * A class for completeness watermark updater.
 * It computes the new watermarks and updates below entities:
 *  1. the properties in {@link IcebergMetadataWriter.TableMetadata}
 *  2. {@link gobblin.configuration.State}
 *  3. the completionWatermark in {@link IcebergMetadataWriter.TableMetadata}
 */
@Slf4j
public class CompletenessWatermarkUpdater {
  private final String topic;
  private final String auditCheckGranularity;

  protected final String timeZone;
  protected final IcebergMetadataWriter.TableMetadata tableMetadata;
  protected final Map<String, String> propsToUpdate;
  protected final State stateToUpdate;
  protected KafkaAuditCountVerifier auditCountVerifier;

  public CompletenessWatermarkUpdater(String topic, String auditCheckGranularity, String timeZone,
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
   * @param includeTotalCountCompletionWatermark If totalCountCompletionWatermark should be calculated
   */
  public void run(SortedSet<ZonedDateTime> timestamps, boolean includeTotalCountCompletionWatermark) {
    String tableName = tableMetadata.table.get().name();
    if (this.topic == null) {
      log.error(String.format("Not performing audit check. %s is null. Please set as table property of %s",
          TOPIC_NAME_KEY, tableName));
    }
    computeAndUpdateWatermark(tableName, timestamps, includeTotalCountCompletionWatermark);
  }

  private void computeAndUpdateWatermark(String tableName, SortedSet<ZonedDateTime> timestamps,
      boolean includeTotalCountWatermark) {
    log.info(String.format("Compute completion watermark for %s and timestamps %s with previous watermark %s, previous totalCount watermark %s, includeTotalCountWatermark=%b",
        this.topic, timestamps, tableMetadata.completionWatermark, tableMetadata.totalCountCompletionWatermark,
        includeTotalCountWatermark));

    WatermarkUpdaterSet updaterSet = new WatermarkUpdaterSet(this.tableMetadata, this.timeZone, this.propsToUpdate,
        this.stateToUpdate, includeTotalCountWatermark);
    if(timestamps == null || timestamps.size() <= 0) {
      log.error("Cannot create time iterator. Empty for null timestamps");
      return;
    }

    ZonedDateTime now = ZonedDateTime.now(ZoneId.of(this.timeZone));
    TimeIterator.Granularity granularity = TimeIterator.Granularity.valueOf(this.auditCheckGranularity);
    ZonedDateTime startDT = timestamps.first();
    ZonedDateTime endDT = timestamps.last();
    TimeIterator iterator = new TimeIterator(startDT, endDT, granularity, true);
    try {
      while (iterator.hasNext()) {
        ZonedDateTime timestampDT = iterator.next();
        updaterSet.checkForEarlyStop(timestampDT, now, granularity);
        if (updaterSet.allFinished()) {
          break;
        }

        ZonedDateTime auditCountCheckLowerBoundDT = TimeIterator.dec(timestampDT, granularity, 1);
        Map<KafkaAuditCountVerifier.CompletenessType, Boolean> results =
            this.auditCountVerifier.calculateCompleteness(this.topic,
                auditCountCheckLowerBoundDT.toInstant().toEpochMilli(),
                timestampDT.toInstant().toEpochMilli());

        updaterSet.computeAndUpdate(results, timestampDT);
      }
    } catch (IOException e) {
      log.warn("Exception during audit count check: ", e);
    }
  }

  /**
   * A class that contains both ClassicWatermakrUpdater and TotalCountWatermarkUpdater
   */
  static class WatermarkUpdaterSet {
    private final List<WatermarkUpdater> updaters;

    WatermarkUpdaterSet(IcebergMetadataWriter.TableMetadata tableMetadata, String timeZone,
        Map<String, String> propsToUpdate, State stateToUpdate, boolean includeTotalCountWatermark) {
      this.updaters = new ArrayList<>();
      this.updaters.add(new ClassicWatermarkUpdater(tableMetadata.completionWatermark, timeZone, tableMetadata,
          propsToUpdate, stateToUpdate));
      if (includeTotalCountWatermark) {
        this.updaters.add(new TotalCountWatermarkUpdater(tableMetadata.totalCountCompletionWatermark, timeZone,
            tableMetadata, propsToUpdate, stateToUpdate));
      }
    }

    void checkForEarlyStop(ZonedDateTime timestampDT, ZonedDateTime now,
        TimeIterator.Granularity granularity) {
      this.updaters.stream().forEach(updater
          -> updater.checkForEarlyStop(timestampDT, now, granularity));
    }

    boolean allFinished() {
      return this.updaters.stream().allMatch(updater -> updater.isFinished());
    }

    void computeAndUpdate(Map<KafkaAuditCountVerifier.CompletenessType, Boolean> results,
        ZonedDateTime timestampDT) {
      this.updaters.stream()
          .filter(updater -> !updater.isFinished())
          .forEach(updater -> updater.computeAndUpdate(results, timestampDT));
    }
  }

  /**
   * A stateful class for watermark updaters.
   * The updater starts with finished=false state.
   * Then computeAndUpdate() is called multiple times with the parameters:
   * 1. The completeness audit results within (datepartition-1, datepartition)
   * 2. the datepartition timestamp
   * The method is call multiple times in descending order of the datepartition timestamp.
   * <p>
   * When the audit result is complete for a timestamp, it updates below entities:
   *    1. the properties in {@link IcebergMetadataWriter.TableMetadata}
   *    2. {@link gobblin.configuration.State}
   *    3. the completionWatermark in {@link IcebergMetadataWriter.TableMetadata}
   *  And it turns into finished=true state, in which the following computeAndUpdate() calls will be skipped.
   */
  static abstract class WatermarkUpdater {
    protected final long previousWatermark;
    protected final ZonedDateTime prevWatermarkDT;
    protected final String timeZone;
    protected boolean finished = false;
    protected final IcebergMetadataWriter.TableMetadata tableMetadata;
    protected final Map<String, String> propsToUpdate;
    protected final State stateToUpdate;

    public WatermarkUpdater(long previousWatermark, String timeZone, IcebergMetadataWriter.TableMetadata tableMetadata,
        Map<String, String> propsToUpdate, State stateToUpdate) {
      this.previousWatermark = previousWatermark;
      this.timeZone = timeZone;
      this.tableMetadata = tableMetadata;
      this.propsToUpdate = propsToUpdate;
      this.stateToUpdate = stateToUpdate;

      prevWatermarkDT = Instant.ofEpochMilli(previousWatermark).atZone(ZoneId.of(this.timeZone));
    }

    public void computeAndUpdate(Map<KafkaAuditCountVerifier.CompletenessType, Boolean> results,
        ZonedDateTime timestampDT) {
      if (finished) {
        return;
      }
      computeAndUpdateInternal(results, timestampDT);
    }

    protected abstract void computeAndUpdateInternal(Map<KafkaAuditCountVerifier.CompletenessType, Boolean> results,
        ZonedDateTime timestampDT);

    protected boolean isFinished() {
      return this.finished;
    }

    protected void setFinished() {
      this.finished = true;
    }

    protected void checkForEarlyStop(ZonedDateTime timestampDT, ZonedDateTime now,
        TimeIterator.Granularity granularity) {
      if (isFinished()
          || (timestampDT.isAfter(this.prevWatermarkDT)
              && TimeIterator.durationBetween(this.prevWatermarkDT, now, granularity) > 0)) {
        return;
      }
      setFinished();
    }
  }

  @VisibleForTesting
  void setAuditCountVerifier(KafkaAuditCountVerifier auditCountVerifier) {
    this.auditCountVerifier = auditCountVerifier;
  }

  static class ClassicWatermarkUpdater extends WatermarkUpdater {
    public ClassicWatermarkUpdater(long previousWatermark, String timeZone,
        IcebergMetadataWriter.TableMetadata tableMetadata, Map<String, String> propsToUpdate, State stateToUpdate) {
      super(previousWatermark, timeZone, tableMetadata, propsToUpdate, stateToUpdate);
    }

    @Override
    protected void computeAndUpdateInternal(Map<KafkaAuditCountVerifier.CompletenessType, Boolean> results,
        ZonedDateTime timestampDT) {
      if (!results.getOrDefault(KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness, false)) {
        return;
      }

      setFinished();
      long updatedWatermark = timestampDT.toInstant().toEpochMilli();
      this.stateToUpdate.setProp(
          String.format(STATE_COMPLETION_WATERMARK_KEY_OF_TABLE,
              this.tableMetadata.table.get().name().toLowerCase(Locale.ROOT)),
          updatedWatermark);

      if (updatedWatermark > this.previousWatermark) {
        log.info(String.format("Updating %s for %s from %s to %s", COMPLETION_WATERMARK_KEY,
            this.tableMetadata.table.get().name(), this.previousWatermark, updatedWatermark));
        this.propsToUpdate.put(COMPLETION_WATERMARK_KEY, String.valueOf(updatedWatermark));
        this.propsToUpdate.put(COMPLETION_WATERMARK_TIMEZONE_KEY, this.timeZone);

        this.tableMetadata.completionWatermark = updatedWatermark;
      }
    }
  }

  static class TotalCountWatermarkUpdater extends WatermarkUpdater {
    public TotalCountWatermarkUpdater(long previousWatermark, String timeZone,
        IcebergMetadataWriter.TableMetadata tableMetadata, Map<String, String> propsToUpdate, State stateToUpdate) {
      super(previousWatermark, timeZone, tableMetadata, propsToUpdate, stateToUpdate);
    }

    @Override
    protected void computeAndUpdateInternal(Map<KafkaAuditCountVerifier.CompletenessType, Boolean> results,
        ZonedDateTime timestampDT) {
      if (!results.getOrDefault(KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness, false)) {
        return;
      }

      setFinished();
      long updatedWatermark = timestampDT.toInstant().toEpochMilli();
      this.stateToUpdate.setProp(
          String.format(STATE_TOTAL_COUNT_COMPLETION_WATERMARK_KEY_OF_TABLE,
              this.tableMetadata.table.get().name().toLowerCase(Locale.ROOT)),
          updatedWatermark);

      if (updatedWatermark > previousWatermark) {
        log.info(String.format("Updating %s for %s from %s to %s", TOTAL_COUNT_COMPLETION_WATERMARK_KEY,
            this.tableMetadata.table.get().name(), previousWatermark, updatedWatermark));
        this.propsToUpdate.put(TOTAL_COUNT_COMPLETION_WATERMARK_KEY, String.valueOf(updatedWatermark));
        tableMetadata.totalCountCompletionWatermark = updatedWatermark;
      }
    }
  }

}
