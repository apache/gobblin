/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract;

import gobblin.source.extractor.JobCommitPolicy;
import gobblin.source.extractor.partition.Partitioner;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.WatermarkPredicate;
import gobblin.source.extractor.watermark.WatermarkType;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.WorkUnit;


/**
 * A base implementation of {@link gobblin.source.Source} for
 * query-based sources.
 */
public abstract class QueryBasedSource<S, D> extends AbstractSource<S, D> {

  private static final Logger LOG = LoggerFactory.getLogger(QueryBasedSource.class);

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    initLogger(state);

    List<WorkUnit> workUnits = Lists.newArrayList();
    String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);

    String extractTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
    // If extract table name is not found then use the entity name
    if (StringUtils.isBlank(extractTableName)) {
      extractTableName = Utils.escapeSpecialCharacters(entityName, ConfigurationKeys.ESCAPE_CHARS_IN_TABLE_NAME, "_");
    }

    TableType tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
    long previousWatermark = this.getLatestWatermarkFromMetadata(state);

    Map<Long, Long> sortedPartitions = Maps.newTreeMap();
    sortedPartitions.putAll(new Partitioner(state).getPartitions(previousWatermark));

    // Use extract table name to create extract
    SourceState partitionState = new SourceState();
    partitionState.addAll(state);
    Extract extract = partitionState.createExtract(tableType, nameSpaceName, extractTableName);

    // Setting current time for the full extract
    if (Boolean.valueOf(state.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY))) {
      extract.setFullTrue(System.currentTimeMillis());
    }

    for (Entry<Long, Long> entry : sortedPartitions.entrySet()) {
      partitionState.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, entry.getKey());
      partitionState.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, entry.getValue());
      workUnits.add(partitionState.createWorkUnit(extract));
    }

    LOG.info("Total number of work units for the current run: " + workUnits.size());

    List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
    LOG.info("Total number of incomplete tasks from the previous run: " + previousWorkUnits.size());
    workUnits.addAll(previousWorkUnits);

    return workUnits;
  }

  @Override
  public void shutdown(SourceState state) {
  }

  /**
   * Get latest water mark from previous work unit states.
   *
   * @param state
   *            Source state
   * @return latest water mark (high water mark)
   */
  private long getLatestWatermarkFromMetadata(SourceState state) {
    LOG.debug("Get latest watermark from the previou run");
    long latestWaterMark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;

    List<WorkUnitState> previousWorkUnitStates = state.getPreviousWorkUnitStates();
    List<Long> previousWorkUnitStateHighWatermarks = Lists.newArrayList();
    List<Long> previousWorkUnitLowWatermarks = Lists.newArrayList();

    if (previousWorkUnitStates.isEmpty()) {
      LOG.info("No previous work unit states found; Latest watermark - Default watermark: " + latestWaterMark);
      return latestWaterMark;
    }

    boolean hasFailedRun = false;
    boolean isCommitOnFullSuccess = false;
    boolean isDataProcessedInPreviousRun = false;

    JobCommitPolicy commitPolicy = JobCommitPolicy
        .forName(state.getProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, ConfigurationKeys.DEFAULT_JOB_COMMIT_POLICY));
    if (commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
      isCommitOnFullSuccess = true;
    }

    for (WorkUnitState workUnitState : previousWorkUnitStates) {
      long processedRecordCount = 0;
      LOG.info("State of the previous task: " + workUnitState.getId() + ":" + workUnitState.getWorkingState());
      if (workUnitState.getWorkingState() == WorkingState.FAILED
          || workUnitState.getWorkingState() == WorkingState.CANCELLED
          || workUnitState.getWorkingState() == WorkingState.RUNNING
          || workUnitState.getWorkingState() == WorkingState.PENDING) {
        hasFailedRun = true;
      } else {
        processedRecordCount = workUnitState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED);
        if (processedRecordCount != 0) {
          isDataProcessedInPreviousRun = true;
        }
      }

      LOG.info("Low watermark of the previous task: " + workUnitState.getId() + ":" + workUnitState.getWorkunit()
          .getLowWaterMark());
      LOG.info(
          "High watermark of the previous task: " + workUnitState.getId() + ":" + workUnitState.getHighWaterMark());
      LOG.info("Record count of the previous task: " + processedRecordCount + "\n");

      // Consider high water mark of the previous work unit, if it is
      // extracted any data
      if (processedRecordCount != 0) {
        previousWorkUnitStateHighWatermarks.add(workUnitState.getHighWaterMark());
      }

      previousWorkUnitLowWatermarks.add(this.getLowWatermarkFromWorkUnit(workUnitState));
    }

    // If commit policy is full and it has failed run, get latest water mark
    // as
    // minimum of low water marks from previous states.
    if (isCommitOnFullSuccess && hasFailedRun) {
      long previousLowWatermark = Collections.min(previousWorkUnitLowWatermarks);

      WorkUnitState previousState = previousWorkUnitStates.get(0);
      ExtractType extractType =
          ExtractType.valueOf(previousState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE).toUpperCase());

      // add backup seconds only for snapshot extracts but not for appends
      if (extractType == ExtractType.SNAPSHOT) {
        int backupSecs = previousState.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS, 0);
        String watermarkType = previousState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE);
        latestWaterMark = this.addBackedUpSeconds(previousLowWatermark, backupSecs, watermarkType);
      } else {
        latestWaterMark = previousLowWatermark;
      }

      LOG.info("Previous job was COMMIT_ON_FULL_SUCCESS but it was failed; Latest watermark - "
          + "Min watermark from WorkUnits: " + latestWaterMark);
    }

    // If commit policy is full and there are no failed tasks or commit
    // policy is partial,
    // get latest water mark as maximum of high water marks from previous
    // tasks.
    else {
      if (isDataProcessedInPreviousRun) {
        latestWaterMark = Collections.max(previousWorkUnitStateHighWatermarks);
        LOG.info(
            "Previous run was successful. Latest watermark - Max watermark from WorkUnitStates: " + latestWaterMark);
      } else {
        latestWaterMark = Collections.min(previousWorkUnitLowWatermarks);
        LOG.info("Previous run was successful but no data found. Latest watermark - Min watermark from WorkUnitStates: "
            + latestWaterMark);
      }
    }

    return latestWaterMark;
  }

  private long addBackedUpSeconds(long lowWatermark, int backupSecs, String watermarkType) {
    if (lowWatermark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
      return lowWatermark;
    }
    WatermarkType wmType = WatermarkType.valueOf(watermarkType.toUpperCase());

    switch (wmType) {
      case SIMPLE:
        return lowWatermark + backupSecs;
      default:
        Date lowWaterMarkDate = Utils.toDate(lowWatermark, "yyyyMMddHHmmss");
        return Long
            .parseLong(Utils.dateToString(Utils.addSecondsToDate(lowWaterMarkDate, backupSecs), "yyyyMMddHHmmss"));
    }
  }

  /**
   * Get low water mark from the given work unit state.
   *
   * @param workUnitState
   *            Work unit state
   * @return latest low water mark
   */
  private long getLowWatermarkFromWorkUnit(WorkUnitState workUnitState) {
    String watermarkType = workUnitState
        .getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE);
    long lowWaterMark = workUnitState.getWorkunit().getLowWaterMark();

    if (lowWaterMark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE) {
      return lowWaterMark;
    }

    WatermarkType wmType = WatermarkType.valueOf(watermarkType.toUpperCase());
    int deltaNum = new WatermarkPredicate(wmType).getDeltaNumForNextWatermark();

    switch (wmType) {
      case SIMPLE:
        return lowWaterMark - deltaNum;
      default:
        Date lowWaterMarkDate = Utils.toDate(lowWaterMark, "yyyyMMddHHmmss");
        return Long
            .parseLong(Utils.dateToString(Utils.addSecondsToDate(lowWaterMarkDate, deltaNum * -1), "yyyyMMddHHmmss"));
    }
  }

  /**
   * Initialize the logger.
   *
   * @param state
   *            Source state
   */
  private void initLogger(SourceState state) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(StringUtils.stripToEmpty(state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA)));
    sb.append("_");
    sb.append(StringUtils.stripToEmpty(state.getProp(ConfigurationKeys.SOURCE_ENTITY)));
    sb.append("]");
    MDC.put("sourceInfo", sb.toString());
  }
}
