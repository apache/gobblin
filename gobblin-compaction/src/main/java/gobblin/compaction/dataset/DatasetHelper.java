/*
 * Copyright (C) 2016-2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.dataset;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Optional;

import gobblin.compaction.conditions.RecompactionCondition;
import gobblin.compaction.conditions.OrRecompactionCondition;
import gobblin.compaction.conditions.RecompactionConditionBasedOnFileCount;
import gobblin.compaction.conditions.RecompactionConditionBasedOnRatio;
import gobblin.compaction.conditions.RecompactionConditionBasedOnDuration;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.FileListUtils;
import gobblin.util.RecordCountProvider;
import gobblin.util.recordcount.LateFileRecordCountProvider;

/**
 * A class {@link DatasetHelper} which provides runtime metrics or other helper functions for a given dataset.
 *
 * The class also contains different recompaction conditions {@link RecompactionCondition}, which indicates if a
 * recompaction is needed. These conditions will be examined by {@link gobblin.compaction.mapreduce.MRCompactorJobRunner}
 * after late data was found and copied from inputDir to outputLateDir.
 */

public class DatasetHelper {
  public static final char DATASETS_WITH_DIFFERENT_RECOMPACT_THRESHOLDS_SEPARATOR = ';';
  public static final char DATASETS_WITH_SAME_RECOMPACT_THRESHOLDS_SEPARATOR = ',';
  public static final char DATASETS_AND_RECOMPACT_THRESHOLD_SEPARATOR = ':';

  private final FileSystem fs;
  private final Dataset dataset;
  private final RecordCountProvider inputRecordCountProvider;
  private final RecordCountProvider outputRecordCountProvider;
  private final LateFileRecordCountProvider lateInputRecordCountProvider;
  private final LateFileRecordCountProvider lateOutputRecordCountProvider;
  private final RecompactionCondition condition;
  private final Collection<String> extensions;

  private static final Logger logger = LoggerFactory.getLogger(RecompactionConditionBasedOnDuration.class);

  public DatasetHelper(Dataset dataset, FileSystem fs, Collection<String> extensions) {
    this.extensions = extensions;
    this.fs = fs;
    this.dataset = dataset;
    this.condition = new OrRecompactionCondition()
        .addCondition(new RecompactionConditionBasedOnRatio(this.getOwnRatioThreshold ()))
        .addCondition(new RecompactionConditionBasedOnDuration(this.getOwnDurationThreshold ()))
        .addCondition(new RecompactionConditionBasedOnFileCount(this.getOwnFileCountThreshold ()));

    try {
      this.inputRecordCountProvider = (RecordCountProvider) Class
          .forName(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_INPUT_RECORD_COUNT_PROVIDER,
              MRCompactor.DEFAULT_COMPACTION_INPUT_RECORD_COUNT_PROVIDER))
          .newInstance();
      this.outputRecordCountProvider = (RecordCountProvider) Class
          .forName(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER,
              MRCompactor.DEFAULT_COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER))
          .newInstance();
      this.lateInputRecordCountProvider = new LateFileRecordCountProvider(this.inputRecordCountProvider);
      this.lateOutputRecordCountProvider = new LateFileRecordCountProvider(this.outputRecordCountProvider);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate RecordCountProvider", e);
    }
  }

  private int getOwnFileCountThreshold () {
    int count = this.dataset.jobProps().getPropAsInt(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FILE_NUM,
        MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_FILE_NUM);
    return count;
  }

  private static PeriodFormatter getPeriodFormatter() {
    return new PeriodFormatterBuilder().appendMonths().appendSuffix("m").appendDays().appendSuffix("d").appendHours()
        .appendSuffix("h").appendMinutes().appendSuffix("min").toFormatter();
  }

  private Period getOwnDurationThreshold () {
    String retention = this.dataset.jobProps().getProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_DURATION,
        MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_DURATION);
    Period period = DatasetHelper.getPeriodFormatter().parsePeriod(retention);
    return period;
  }

  private double getOwnRatioThreshold() {
    Map<String, Double> datasetRegexAndRecompactThreshold = getDatasetRegexAndRecompactThreshold(
        this.dataset.jobProps().getProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET, StringUtils.EMPTY));
    for (Map.Entry<String, Double> topicRegexEntry : datasetRegexAndRecompactThreshold.entrySet()) {
      if (DatasetFilterUtils.stringInPatterns(dataset.getDatasetName(),
          DatasetFilterUtils.getPatternsFromStrings(Splitter.on(DATASETS_WITH_SAME_RECOMPACT_THRESHOLDS_SEPARATOR)
              .trimResults().omitEmptyStrings().splitToList(topicRegexEntry.getKey())))) {
        return topicRegexEntry.getValue();
      }
    }
    return MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET;
  }

  private static Map<String, Double> getDatasetRegexAndRecompactThreshold(String datasetsAndRecompactThresholds) {
    Map<String, Double> topicRegexAndRecompactThreshold = Maps.newHashMap();
    for (String entry : Splitter.on(DATASETS_WITH_DIFFERENT_RECOMPACT_THRESHOLDS_SEPARATOR).trimResults()
        .omitEmptyStrings().splitToList(datasetsAndRecompactThresholds)) {
      List<String> topicsAndRecompactThreshold =
          Splitter.on(DATASETS_AND_RECOMPACT_THRESHOLD_SEPARATOR).trimResults().omitEmptyStrings().splitToList(entry);
      if (topicsAndRecompactThreshold.size() != 2) {
        logger.error("Invalid form (DATASET_NAME:THRESHOLD) in "
            + MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET + ".");
      } else {
        topicRegexAndRecompactThreshold.put(topicsAndRecompactThreshold.get(0),
            Double.parseDouble(topicsAndRecompactThreshold.get(1)));
      }
    }
    return topicRegexAndRecompactThreshold;
  }

  private List<Path> getApplicableFilePaths (Path dataDir) throws IOException {
    if (!this.fs.exists(dataDir)) {
      return Lists.newArrayList();
    }
    List<Path> paths = Lists.newArrayList();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(this.fs, dataDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        for (String validExtention : extensions) {
          if (path.getName().endsWith(validExtention)) {
            return true;
          }
        }
        return false;
      }
    })) {
      paths.add(fileStatus.getPath());
    }
    return paths;
  }

  public Optional<DateTime> getEarliestLateFileModificationTime() {
    DateTimeZone timeZone = DateTimeZone
        .forID(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    try {
      long maxTimestamp = Long.MIN_VALUE;
      for (FileStatus status : FileListUtils.listFilesRecursively(this.fs, this.dataset.outputLatePath())) {
        maxTimestamp = Math.max(maxTimestamp, status.getModificationTime());
      }
      return maxTimestamp == Long.MIN_VALUE ? Optional.<DateTime>absent():Optional.of(new DateTime(maxTimestamp, timeZone));
    } catch (Exception e) {
      logger.error("Failed to get earliest late file modification time");
      return Optional.absent();
    }
  }

  public DateTime getCurrentTime() {
    DateTimeZone timeZone = DateTimeZone
        .forID(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    DateTime currentTime = new DateTime(timeZone);
    return currentTime;
  }

  public long getLateOutputRecordCount() {
    long lateOutputRecordCount = 0l;
    try {
      Path outputLatePath = dataset.outputLatePath();
      if (this.fs.exists(outputLatePath)) {
        lateOutputRecordCount = this.lateOutputRecordCountProvider
            .getRecordCount(this.getApplicableFilePaths(dataset.outputLatePath()));
      }
    } catch (Exception e) {
      logger.error("Failed to get late record count:" + e, e);
    }
    return lateOutputRecordCount;
  }

  public long getOutputRecordCount() {
    long outputRecordCount = 01;
    try {
      outputRecordCount = this.outputRecordCountProvider.
          getRecordCount(this.getApplicableFilePaths(dataset.outputPath()));
      return outputRecordCount;
    } catch (Exception e) {
      logger.error("Failed to submit late event count:" + e, e);
    }
    return outputRecordCount;
  }

  protected RecompactionCondition getCondition() {
    return condition;
  }

  public long getLateOutputFileCount() {
      long lateOutputFileCount = 0l;
      try {
        Path outputLatePath = dataset.outputLatePath();
        if (this.fs.exists(outputLatePath)) {
          lateOutputFileCount = getApplicableFilePaths(dataset.outputLatePath()).size();
          logger.info("LateOutput File Count is : " + lateOutputFileCount + " at " + outputLatePath.toString());
        }
      } catch (Exception e) {
        logger.error("Failed to get late file count from :" + e, e);
      }
      return lateOutputFileCount;
  }
}

