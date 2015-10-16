/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.mapreduce;

import java.io.IOException;
import java.util.List;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import gobblin.compaction.Dataset;
import gobblin.compaction.event.CompactionSlaEventHelper;


/**
 * This class creates MR job properties for time-based compaction, i.e., the input folder pattern
 * contains a timestamp, such as PageViewEvent/hourly/2015/05/22.
 *
 * The folder pattern can be configured by compaction.timebased.folder.pattern, the default value
 * being YYYY/MM/dd, which means an MR job will be launched for each qualified folder that matches
 * [this.inputPath]&#47;*&#47;*&#47;*.
 *
 * To control which folders to process, use properties compaction.timebased.min.time.ago and
 * compaction.timebased.max.time.ago. The format is ?m?d?h, e.g., 3m or 2d10h.
 *
 * @author ziliu
 */
public class MRCompactorTimeBasedJobPropCreator extends MRCompactorJobPropCreator {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorTimeBasedJobPropCreator.class);

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

  // Properties used internally
  public static final String COMPACTION_TIMEBASED_INPUT_PATH_TIME = COMPACTION_TIMEBASED_PREFIX + "input.path.time";

  private final String folderTimePattern;
  private final DateTimeZone timeZone;
  private final DateTimeFormatter timeFormatter;

  static class Builder extends MRCompactorJobPropCreator.Builder<MRCompactorTimeBasedJobPropCreator.Builder> {
    @Override
    MRCompactorTimeBasedJobPropCreator build() {
      return new MRCompactorTimeBasedJobPropCreator(this);
    }
  }

  MRCompactorTimeBasedJobPropCreator(Builder builder) {
    super(builder);
    this.folderTimePattern = getFolderPattern();
    this.timeZone = DateTimeZone
        .forID(this.state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    this.timeFormatter = DateTimeFormat.forPattern(this.folderTimePattern).withZone(this.timeZone);
  }

  @Override
  protected List<Dataset> createJobProps() throws IOException {
    List<Dataset> datasets = Lists.newArrayList();
    if (!fs.exists(this.topicInputDir)) {
      LOG.warn("Input folder " + this.topicInputDir + " does not exist. Skipping topic " + topic);
      return datasets;
    }

    String folderStructure = getFolderStructure();
    for (FileStatus status : this.fs.globStatus(new Path(this.topicInputDir, folderStructure))) {
      Path jobInputPath = status.getPath();
      DateTime folderTime = null;
      try {
        folderTime = getFolderTime(jobInputPath);
      } catch (RuntimeException e) {
        LOG.warn(jobInputPath + " is not a valid folder. Will be skipped.");
        continue;
      }

      if (folderWithinAllowedPeriod(jobInputPath, folderTime)) {
        Path jobInputLatePath = appendFolderTime(this.topicInputLateDir, folderTime);
        Path jobOutputPath = appendFolderTime(this.topicOutputDir, folderTime);
        Path jobOutputLatePath = appendFolderTime(this.topicOutputLateDir, folderTime);
        Path jobOutputTmpPath = appendFolderTime(this.topicTmpOutputDir, folderTime);

        Dataset dataset = new Dataset.Builder().withTopic(this.topic).withPriority(this.priority)
            .withInputPath(this.recompactFromOutputPaths ? jobOutputPath : jobInputPath)
            .withInputLatePath(this.recompactFromOutputPaths ? jobOutputLatePath : jobInputLatePath)
            .withOutputPath(jobOutputPath).withOutputLatePath(jobOutputLatePath).withOutputTmpPath(jobOutputTmpPath)
            .build();

        Optional<Dataset> datasetWithJobProps = createJobProps(dataset, folderTime.toString(this.timeFormatter));
        if (datasetWithJobProps.isPresent()) {
          datasetWithJobProps.get().jobProps().setProp(COMPACTION_TIMEBASED_INPUT_PATH_TIME, folderTime.getMillis());
          datasets.add(datasetWithJobProps.get());
          if (this.recompactFromOutputPaths || !MRCompactor.datasetAlreadyCompacted(this.fs, dataset)) {

            // Set the upstream time to partition + 1 day. E.g. for 2015/10/13 the upstream time is midnight of 2015/10/14
            CompactionSlaEventHelper.setUpstreamTimeStamp(state, folderTime.plusDays(1).getMillis());
          }
        }
      }
    }

    return datasets;
  }

  private String getFolderStructure() {
    return this.folderTimePattern.replaceAll("[a-zA-Z0-9]+", "*");
  }

  private String getFolderPattern() {
    String folderPattern =
        this.state.getProp(COMPACTION_TIMEBASED_FOLDER_PATTERN, DEFAULT_COMPACTION_TIMEBASED_FOLDER_PATTERN);
    LOG.info("Compaction folder pattern: " + folderPattern);
    return folderPattern;
  }

  private DateTime getFolderTime(Path path) {
    int startPos = path.toString().indexOf(this.topicInputDir.toString()) + this.topicInputDir.toString().length();
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
      LOG.info(String.format("Folder time for %s is %s, earlier than the earliest allowed folder time, %s. Skipping",
          inputFolder, folderTime, earliestAllowedFolderTime));
      return false;
    } else if (folderTime.isAfter(latestAllowedFolderTime)) {
      LOG.info(String.format("Folder time for %s is %s, later than the latest allowed folder time, %s. Skipping",
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
