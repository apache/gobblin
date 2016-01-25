/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.util.DatasetFilterUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * An implementation {@link DatasetsFinder} based on time-based subdirs of the inputDir.
 *
 * {@link #inputDir} may contain multiple datasets. The path must follow some subdir and time-based pattern,
 *  which can be configured by compaction.*.subdir and compaction.timebased.folder.pattern.
 *  For example, the subdir name is 'daily' and time-based patterhn is 'YYYY/MM/dd'.
 *  A dataset will be created for each qualified folder that matches '[intputDir]/datasetName/daily/YYYY/MM/dd'.
 *
 * Dataset name is used for blacklist/whitelist, and finding high/normal priorities, and recompaction threshold.
 *
 * To control which folders to process, use properties compaction.timebased.min.time.ago and
 * compaction.timebased.max.time.ago. The format is ?m?d?h, e.g., 3m or 2d10h.
 */
@Slf4j
public class TimeBasedSubDirDatasetsFinder extends DatasetsFinder {
  private static final String COMPACTION_TIMEBASED_PREFIX = "compaction.timebased.";

  /**
   * Configuration properties related to time based compaction jobs.
   */
  private static final String COMPACTION_TIMEBASED_FOLDER_PATTERN = COMPACTION_TIMEBASED_PREFIX + "folder.pattern";
  private static final String DEFAULT_COMPACTION_TIMEBASED_FOLDER_PATTERN = "YYYY/MM/dd";

  // The earliest dataset timestamp to be processed. Format = ?m?d?h.
  private static final String COMPACTION_TIMEBASED_MAX_TIME_AGO = COMPACTION_TIMEBASED_PREFIX + "max.time.ago";
  private static final String DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO = "3d";

  // The latest dataset timestamp to be processed. Format = ?m?d?h.
  private static final String COMPACTION_TIMEBASED_MIN_TIME_AGO = COMPACTION_TIMEBASED_PREFIX + "min.time.ago";
  private static final String DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO = "1d";

  private final String folderTimePattern;
  private final DateTimeZone timeZone;
  private final DateTimeFormatter timeFormatter;
  private final String inputSubDir;
  private final String inputLateSubDir;
  private final String destSubDir;
  private final String destLateSubDir;

  public TimeBasedSubDirDatasetsFinder(State state) throws Exception {
    super(state);
    this.inputSubDir = getInputSubDir();
    this.inputLateSubDir = getInputLateSubDir();
    this.destSubDir = getDestSubDir();
    this.destLateSubDir = getDestLateSubDir();
    this.folderTimePattern = getFolderPattern();
    this.timeZone =
        DateTimeZone
            .forID(this.state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    this.timeFormatter = DateTimeFormat.forPattern(this.folderTimePattern).withZone(this.timeZone);
  }

  /**
   * Each subdir in {@link DatasetsFinder#inputDir} is considered a dataset, if it satisfies blacklist and whitelist.
   */
  @Override
  public Set<Dataset> findDistinctDatasets() throws IOException {
    Set<Dataset> datasets = Sets.newHashSet();
    for (FileStatus datasetsFileStatus : this.fs.listStatus(new Path(this.inputDir))) {
      if (datasetsFileStatus.isDir()) {
        String datasetName = datasetsFileStatus.getPath().getName();
        if (DatasetFilterUtils.survived(datasetName, this.blacklist, this.whitelist)) {
          log.info("Found dataset: " + datasetsFileStatus.getPath().getName());
          Path inputPath = new Path(this.inputDir, new Path(datasetName, this.inputSubDir));
          Path inputLatePath = new Path(this.inputDir, new Path(datasetName, this.inputLateSubDir));
          Path outputPath = new Path(this.destDir, new Path(datasetName, this.destSubDir));
          Path outputLatePath = new Path(this.destDir, new Path(datasetName, this.destLateSubDir));
          Path outputTmpPath = new Path(this.tmpOutputDir, new Path(datasetName, this.destSubDir));
          double priority = this.getDatasetPriority(datasetName);
          double lateDataThresholdForRecompact = this.getDatasetRecompactThreshold(datasetName);

          String folderStructure = getFolderStructure();
          for (FileStatus status : this.fs.globStatus(new Path(inputPath, folderStructure))) {
            Path jobInputPath = status.getPath();
            DateTime folderTime = null;
            try {
              folderTime = getFolderTime(jobInputPath, inputPath);
            } catch (RuntimeException e) {
              log.warn(jobInputPath + " is not a valid folder. Will be skipped.");
              continue;
            }

            if (folderWithinAllowedPeriod(jobInputPath, folderTime)) {
              Path jobInputLatePath = appendFolderTime(inputLatePath, folderTime);
              Path jobOutputPath = appendFolderTime(outputPath, folderTime);
              Path jobOutputLatePath = appendFolderTime(outputLatePath, folderTime);
              Path jobOutputTmpPath = appendFolderTime(outputTmpPath, folderTime);

              Dataset timeBasedDataset =
                  new Dataset.Builder().withPriority(priority)
                      .withLateDataThresholdForRecompact(lateDataThresholdForRecompact)
                      .withInputPath(this.recompactDatasets ? jobOutputPath : jobInputPath)
                      .withInputLatePath(this.recompactDatasets ? jobOutputLatePath : jobInputLatePath)
                      .withOutputPath(jobOutputPath).withOutputLatePath(jobOutputLatePath)
                      .withOutputTmpPath(jobOutputTmpPath).build();
              // Stores the extra information for timeBasedDataset
              timeBasedDataset.setJobProp(MRCompactor.COMPACTION_JOB_DEST_PARTITION,
                  folderTime.toString(this.timeFormatter));
              timeBasedDataset.setJobProp(MRCompactor.COMPACTION_INPUT_PATH_TIME, folderTime.getMillis());
              datasets.add(timeBasedDataset);
            }
          }
        }
      }
    }
    return datasets;
  }

  private String getInputSubDir() {
    return this.state.getProp(MRCompactor.COMPACTION_INPUT_SUBDIR, MRCompactor.DEFAULT_COMPACTION_INPUT_SUBDIR);
  }

  private String getInputLateSubDir() {
    return this.state.getProp(MRCompactor.COMPACTION_INPUT_SUBDIR, MRCompactor.DEFAULT_COMPACTION_INPUT_SUBDIR)
        + MRCompactor.COMPACTION_LATE_DIR_SUFFIX;
  }

  private String getDestLateSubDir() {
    return this.state.getProp(MRCompactor.COMPACTION_DEST_SUBDIR, MRCompactor.DEFAULT_COMPACTION_DEST_SUBDIR)
        + MRCompactor.COMPACTION_LATE_DIR_SUFFIX;
  }

  private String getDestSubDir() {
    return this.state.getProp(MRCompactor.COMPACTION_DEST_SUBDIR, MRCompactor.DEFAULT_COMPACTION_DEST_SUBDIR);
  }

  private String getFolderStructure() {
    return this.folderTimePattern.replaceAll("[a-zA-Z0-9]+", "*");
  }

  private String getFolderPattern() {
    String folderPattern =
        this.state.getProp(COMPACTION_TIMEBASED_FOLDER_PATTERN, DEFAULT_COMPACTION_TIMEBASED_FOLDER_PATTERN);
    log.info("Compaction folder pattern: " + folderPattern);
    return folderPattern;
  }

  private DateTime getFolderTime(Path path, Path basePath) {
    int startPos = path.toString().indexOf(basePath.toString()) + basePath.toString().length();
    return this.timeFormatter.parseDateTime(StringUtils.removeStart(path.toString().substring(startPos), "/"));
  }

  /**
   * Return true iff input folder time is between compaction.timebased.min.time.ago and
   * compaction.timebased.max.time.ago.
   */
  private boolean folderWithinAllowedPeriod(Path inputFolder, DateTime folderTime) {
    DateTime currentTime = new DateTime(this.timeZone);
    PeriodFormatter periodFormatter = getPeriodFormatter();
    DateTime earliestAllowedFolderTime = getEarliestAllowedFolderTime(currentTime, periodFormatter);
    DateTime latestAllowedFolderTime = getLatestAllowedFolderTime(currentTime, periodFormatter);

    if (folderTime.isBefore(earliestAllowedFolderTime)) {
      log.info(String.format("Folder time for %s is %s, earlier than the earliest allowed folder time, %s. Skipping",
          inputFolder, folderTime, earliestAllowedFolderTime));
      return false;
    } else if (folderTime.isAfter(latestAllowedFolderTime)) {
      log.info(String.format("Folder time for %s is %s, later than the latest allowed folder time, %s. Skipping",
          inputFolder, folderTime, latestAllowedFolderTime));
      return false;
    } else {
      return true;
    }
  }

  private PeriodFormatter getPeriodFormatter() {
    return new PeriodFormatterBuilder().appendMonths().appendSuffix("m").appendDays().appendSuffix("d").appendHours()
        .appendSuffix("h").toFormatter();
  }

  private DateTime getEarliestAllowedFolderTime(DateTime currentTime, PeriodFormatter periodFormatter) {
    String maxTimeAgoStr =
        this.state.getProp(COMPACTION_TIMEBASED_MAX_TIME_AGO, DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO);
    Period maxTimeAgo = periodFormatter.parsePeriod(maxTimeAgoStr);
    return currentTime.minus(maxTimeAgo);
  }

  private DateTime getLatestAllowedFolderTime(DateTime currentTime, PeriodFormatter periodFormatter) {
    String minTimeAgoStr =
        this.state.getProp(COMPACTION_TIMEBASED_MIN_TIME_AGO, DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO);
    Period minTimeAgo = periodFormatter.parsePeriod(minTimeAgoStr);
    return currentTime.minus(minTimeAgo);
  }

  private Path appendFolderTime(Path path, DateTime folderTime) {
    return new Path(path, folderTime.toString(this.timeFormatter));
  }
}
