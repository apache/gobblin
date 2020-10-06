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

package org.apache.gobblin.compaction.action;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.compaction.verify.CompactionWatermarkChecker;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.time.TimeIterator;


/**
 * The class publishes compaction watermarks, reported by {@link CompactionWatermarkChecker}, as hive table parameters.
 * It guarantees compaction watermark is updated continuously and errors out if there is a gap, which indicates a
 * compaction hole. At the time of writing, one should manually fill the compaction hole and update the existing
 * watermarks in hive table parameters to recover automatic watermark publish
 */
@Slf4j
public class CompactionWatermarkAction implements CompactionCompleteAction<FileSystemDataset> {

  public static final String CONF_PREFIX = "compactionWatermarkAction";
  public static final String GRANULARITY = CONF_PREFIX + ".granularity";
  public static final String DEFAULT_HIVE_DB = CONF_PREFIX + ".defaultHiveDb";

  private EventSubmitter submitter;
  private State state;
  private final String defaultHiveDb;
  private final TimeIterator.Granularity granularity;
  private final ZoneId zone;

  public CompactionWatermarkAction(State state) {
    this.state = state;
    defaultHiveDb = state.getProp(DEFAULT_HIVE_DB);
    granularity = TimeIterator.Granularity.valueOf(state.getProp(GRANULARITY).toUpperCase());
    zone = ZoneId.of(state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
  }

  @Override
  public void onCompactionJobComplete(FileSystemDataset dataset)
      throws IOException {

    String compactionWatermark = state.getProp(CompactionWatermarkChecker.COMPACTION_WATERMARK);
    String completeCompactionWatermark = state.getProp(CompactionWatermarkChecker.COMPLETION_COMPACTION_WATERMARK);
    if (StringUtils.isEmpty(compactionWatermark) && StringUtils.isEmpty(completeCompactionWatermark)) {
      return;
    }

    CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
    HiveDatasetFinder.DbAndTable dbAndTable = extractDbTable(result.getDatasetName());
    String hiveDb = dbAndTable.getDb();
    String hiveTable = dbAndTable.getTable();

    try (HiveRegister hiveRegister = HiveRegister.get(state)) {
      Optional<HiveTable> tableOptional = hiveRegister.getTable(hiveDb, hiveTable);
      if (!tableOptional.isPresent()) {
        log.info("Table {}.{} not found. Skip publishing compaction watermarks", hiveDb, hiveTable);
        return;
      }

      HiveTable table = tableOptional.get();
      State tableProps = table.getProps();
      boolean shouldUpdate = mayUpdateWatermark(dataset, tableProps, CompactionWatermarkChecker.COMPACTION_WATERMARK, compactionWatermark);
      if (mayUpdateWatermark(dataset, tableProps, CompactionWatermarkChecker.COMPLETION_COMPACTION_WATERMARK,
          completeCompactionWatermark)) {
        shouldUpdate = true;
      }

      if (shouldUpdate) {
        log.info("Alter table {}.{} to publish watermarks {}", hiveDb, hiveTable, tableProps);
        hiveRegister.alterTable(table);
      }
    }
  }

  /**
   * Update watermark if the new one is continuously higher than the existing one
   */
  private boolean mayUpdateWatermark(FileSystemDataset dataset, State props, String key, String newValue) {

    if (StringUtils.isEmpty(newValue)) {
      return false;
    }

    long existing = props.getPropAsLong(key, 0);
    if (existing == 0) {
      props.setProp(key, newValue);
      return true;
    }

    long actualNextWatermark = Long.parseLong(newValue);
    if (actualNextWatermark <= existing) {
      return false;
    }

    long expectedWatermark = getExpectedNextWatermark(existing);
    if (actualNextWatermark != expectedWatermark) {
      String errMsg = String.format(
          "Fail to advance %s of dataset %s: expect %s but got %s, please manually fill the gap and rerun.",
          key, dataset.datasetRoot(), expectedWatermark, actualNextWatermark);
      log.error(errMsg);
      throw new RuntimeException(errMsg);
    }

    props.setProp(key, newValue);
    return true;
  }

  /**
   * To guarantee watermark continuity, the expected next watermark should be: {@code previousWatermark} + 1
   * unit of {@link #granularity}
   */
  private long getExpectedNextWatermark(Long previousWatermark) {
    ZonedDateTime previousWatermarkTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(previousWatermark), zone);
    // Since version 1.8, java time supports DST change in PST(America/Los_Angeles) time zone
    ZonedDateTime nextWatermarkTime = TimeIterator.inc(previousWatermarkTime, granularity, 1);
    return nextWatermarkTime.toInstant().toEpochMilli();
  }

  @Override
  public void addEventSubmitter(EventSubmitter submitter) {
    this.submitter = submitter;
  }

  private HiveDatasetFinder.DbAndTable extractDbTable(String datasetName) {
    String[] parts = datasetName.split("/");
    if (parts.length == 0 || parts.length > 2) {
      throw new RuntimeException(String.format("Unsupported dataset %s", datasetName));
    }
    String hiveDb = defaultHiveDb;
    String hiveTable = parts[0];
    // Use the db from the datasetName if it has
    if (parts.length == 2) {
      hiveDb = parts[0];
      hiveTable = parts[1];
    }

    return new HiveDatasetFinder.DbAndTable(hiveDb, hiveTable);
  }
}
