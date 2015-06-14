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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;

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

import com.google.common.collect.Lists;


/**
 * This class creates MR job properties for time-based compaction, i.e., the input folder pattern
 * contains a timestamp, such as PageViewEvent/hourly/2015/05/22.
 *
 * The folder pattern can be configured by compaction.timebased.folder.pattern, the default value
 * being YYYY/MM/dd, which means an MR job will be launched for each qualified folder that matches
 * [this.inputPath]&#47;*&#47;*&#47;*.
 *
 * To control which folders to process, use properties compaction.timebased.max.time.ago and
 * compaction.timebased.max.time.ago. The format is ?m?d?h, e.g., 3m or 2d10h.
 *
 * @author ziliu
 */
public class MRCompactorTimeBasedJobPropCreator extends MRCompactorJobPropCreator {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorTimeBasedJobPropCreator.class);

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
    this.timeZone = DateTimeZone.forID(
        this.state.getProp(ConfigurationKeys.COMPACTION_TIMEZONE, ConfigurationKeys.DEFAULT_COMPACTION_TIMEZONE));
    this.timeFormatter = DateTimeFormat.forPattern(this.folderTimePattern).withZone(this.timeZone);
  }

  @Override
  protected List<State> createJobProps() throws IOException {
    List<State> allJobProps = Lists.newArrayList();
    if (!fs.exists(this.topicInputDir)) {
      LOG.warn("Input folder " + this.topicInputDir + " does not exist. Skipping topic " + topic);
      return allJobProps;
    }

    String folderStructure = getFolderStructure();
    for (FileStatus status : this.fs.globStatus(new Path(this.topicInputDir, folderStructure))) {
      DateTime folderTime = null;
      try {
        folderTime = getFolderTime(status.getPath());
      } catch (RuntimeException e) {
        LOG.warn(status.getPath() + " is not a valid folder. Will be skipped.");
        continue;
      }
      Path jobOutputDir = new Path(this.topicOutputDir, folderTime.toString(this.timeFormatter));
      Path jobTmpDir = new Path(this.topicTmpDir, folderTime.toString(this.timeFormatter));
      if (shouldProcessFolder(status.getPath(), jobOutputDir, folderTime)) {
        allJobProps.add(createJobProps(status.getPath(), jobOutputDir, jobTmpDir));
      }
    }

    return allJobProps;
  }

  private String getFolderStructure() {
    return this.folderTimePattern.replaceAll("[a-zA-Z0-9]+", "*");
  }

  private String getFolderPattern() {
    String folderPattern = this.state.getProp(ConfigurationKeys.COMPACTION_TIMEBASED_FOLDER_PATTERN,
        ConfigurationKeys.DEFAULT_COMPACTION_TIMEBASED_FOLDER_PATTERN);
    LOG.info("Compaction folder pattern: " + folderPattern);
    return folderPattern;
  }

  private DateTime getFolderTime(Path path) {
    int startPos = path.toString().indexOf(this.topicInputDir.toString()) + this.topicInputDir.toString().length();
    return this.timeFormatter.parseDateTime(StringUtils.removeStart(path.toString().substring(startPos), "/"));
  }

  /**
   * A folder should be processed if (1) input folder time is between compaction.timebased.max.time.ago and
   * compaction.timebased.max.time.ago, (2) output folder is not already compacted.
   */
  private boolean shouldProcessFolder(Path inputFolder, Path outputFolder, DateTime folderTime) {
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
    } else if (folderAlreadyCompacted(outputFolder)) {
      LOG.info(String.format("Folder %s already compacted. Skipping", outputFolder));
      return false;
    }
    return true;
  }

  private PeriodFormatter getPeriodFormatter() {
    return new PeriodFormatterBuilder().appendMonths().appendSuffix("m").appendDays().appendSuffix("d").appendHours()
        .appendSuffix("h").toFormatter();
  }

  private DateTime getEarliestAllowedFolderTime(DateTime currentTime, PeriodFormatter periodFormatter) {
    String maxTimeAgoStr = this.state.getProp(ConfigurationKeys.COMPACTION_TIMEBASED_MAX_TIME_AGO,
        ConfigurationKeys.DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO);
    Period maxTimeAgo = periodFormatter.parsePeriod(maxTimeAgoStr);
    return currentTime.minus(maxTimeAgo);
  }

  private DateTime getLatestAllowedFolderTime(DateTime currentTime, PeriodFormatter periodFormatter) {
    String minTimeAgoStr = this.state.getProp(ConfigurationKeys.COMPACTION_TIMEBASED_MIN_TIME_AGO,
        ConfigurationKeys.DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO);
    Period minTimeAgo = periodFormatter.parsePeriod(minTimeAgoStr);
    return currentTime.minus(minTimeAgo);
  }

  private boolean folderAlreadyCompacted(Path outputFolder) {
    Path filePath = new Path(outputFolder, ConfigurationKeys.COMPACTION_COMPLETE_FILE_NAME);
    try {
      return this.fs.exists(filePath);
    } catch (IOException e) {
      LOG.error("Failed to verify the existence of file " + filePath, e);
      return false;
    }
  }

}
