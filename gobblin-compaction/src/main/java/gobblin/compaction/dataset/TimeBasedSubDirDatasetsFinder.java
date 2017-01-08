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

package gobblin.compaction.dataset;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.util.DatasetFilterUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.io.IOException;
import java.util.Set;


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
  public static final String COMPACTION_TIMEBASED_FOLDER_PATTERN = COMPACTION_TIMEBASED_PREFIX + "folder.pattern";
  public static final String DEFAULT_COMPACTION_TIMEBASED_FOLDER_PATTERN = "YYYY/MM/dd";

  public static final String COMPACTION_TIMEBASED_SUBDIR_PATTERN = COMPACTION_TIMEBASED_PREFIX + "subdir.pattern";
  public static final String DEFAULT_COMPACTION_TIMEBASED_SUBDIR_PATTERN = "*";

  // The earliest dataset timestamp to be processed. Format = ?m?d?h.
  public static final String COMPACTION_TIMEBASED_MAX_TIME_AGO = COMPACTION_TIMEBASED_PREFIX + "max.time.ago";
  public static final String DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO = "3d";

  // The latest dataset timestamp to be processed. Format = ?m?d?h.
  public static final String COMPACTION_TIMEBASED_MIN_TIME_AGO = COMPACTION_TIMEBASED_PREFIX + "min.time.ago";
  public static final String DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO = "1d";

  protected final String folderTimePattern;
  protected final String subDirPattern;
  protected final DateTimeZone timeZone;
  protected final DateTimeFormatter timeFormatter;
  protected final String inputSubDir;
  protected final String inputLateSubDir;
  protected final String destSubDir;
  protected final String destLateSubDir;

  @VisibleForTesting
  public TimeBasedSubDirDatasetsFinder(State state, FileSystem fs) throws Exception {
    super(state, fs);
    this.inputSubDir = getInputSubDir();
    this.inputLateSubDir = getInputLateSubDir();
    this.destSubDir = getDestSubDir();
    this.destLateSubDir = getDestLateSubDir();
    this.folderTimePattern = getFolderPattern();
    this.subDirPattern = getSubDirPattern();
    this.timeZone = DateTimeZone
        .forID(this.state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    this.timeFormatter = DateTimeFormat.forPattern(this.folderTimePattern).withZone(this.timeZone);
  }

  public TimeBasedSubDirDatasetsFinder(State state) throws Exception {
    super(state);
    this.inputSubDir = getInputSubDir();
    this.inputLateSubDir = getInputLateSubDir();
    this.destSubDir = getDestSubDir();
    this.destLateSubDir = getDestLateSubDir();
    this.folderTimePattern = getFolderPattern();
    this.subDirPattern = getSubDirPattern();
    this.timeZone = DateTimeZone
        .forID(this.state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    this.timeFormatter = DateTimeFormat.forPattern(this.folderTimePattern).withZone(this.timeZone);
  }

  protected String getDatasetName(String path, String basePath) {
    int startPos = path.indexOf(basePath) + basePath.length();
    return StringUtils.removeStart(path.substring(startPos), "/");
  }

  /**
   * Each subdir in {@link DatasetsFinder#inputDir} is considered a dataset, if it satisfies blacklist and whitelist.
   */
  @Override
  public Set<Dataset> findDistinctDatasets() throws IOException {
    Set<Dataset> datasets = Sets.newHashSet();
    for (FileStatus datasetsFileStatus : this.fs.globStatus(new Path(inputDir, subDirPattern))) {
      log.info("Scanning directory : " + datasetsFileStatus.getPath().toString());
      if (datasetsFileStatus.isDirectory()) {
        String datasetName = getDatasetName(datasetsFileStatus.getPath().toString(), inputDir);
        if (DatasetFilterUtils.survived(datasetName, this.blacklist, this.whitelist)) {
          log.info("Found dataset: " + datasetName);
          Path inputPath = new Path(this.inputDir, new Path(datasetName, this.inputSubDir));
          Path inputLatePath = new Path(this.inputDir, new Path(datasetName, this.inputLateSubDir));
          Path outputPath = new Path(this.destDir, new Path(datasetName, this.destSubDir));
          Path outputLatePath = new Path(this.destDir, new Path(datasetName, this.destLateSubDir));
          Path outputTmpPath = new Path(this.tmpOutputDir, new Path(datasetName, this.destSubDir));
          double priority = this.getDatasetPriority(datasetName);

          String folderStructure = getFolderStructure();
          for (FileStatus status : this.fs.globStatus(new Path(inputPath, folderStructure))) {
            Path jobInputPath = status.getPath();
            DateTime folderTime = null;
            try {
              folderTime = getFolderTime(jobInputPath, inputPath);
            } catch (RuntimeException e) {
              log.warn("{} is not a valid folder. Will be skipped due to exception.", jobInputPath, e);
              continue;
            }

            if (folderWithinAllowedPeriod(jobInputPath, folderTime)) {
              Path jobInputLatePath = appendFolderTime(inputLatePath, folderTime);
              Path jobOutputPath = appendFolderTime(outputPath, folderTime);
              Path jobOutputLatePath = appendFolderTime(outputLatePath, folderTime);
              Path jobOutputTmpPath = appendFolderTime(outputTmpPath, folderTime);

              Dataset timeBasedDataset = new Dataset.Builder().withPriority(priority)
                  .withDatasetName(datasetName)
                  .addInputPath(this.recompactDatasets ? jobOutputPath : jobInputPath)
                  .addInputLatePath(this.recompactDatasets ? jobOutputLatePath : jobInputLatePath)
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

  protected String getFolderStructure() {
    return this.folderTimePattern.replaceAll("[a-zA-Z0-9='-]+", "*");
  }

  private String getFolderPattern() {
    String folderPattern =
        this.state.getProp(COMPACTION_TIMEBASED_FOLDER_PATTERN, DEFAULT_COMPACTION_TIMEBASED_FOLDER_PATTERN);
    log.info("Compaction folder pattern: " + folderPattern);
    return folderPattern;
  }

  private String getSubDirPattern() {
    String subdirPattern =
        this.state.getProp(COMPACTION_TIMEBASED_SUBDIR_PATTERN, DEFAULT_COMPACTION_TIMEBASED_SUBDIR_PATTERN);
    log.info("Compaction subdir pattern: " + subdirPattern);
    return subdirPattern;
  }

  protected DateTime getFolderTime(Path path, Path basePath) {
    int startPos = path.toString().indexOf(basePath.toString()) + basePath.toString().length();
    return this.timeFormatter.parseDateTime(StringUtils.removeStart(path.toString().substring(startPos), "/"));
  }

  /**
   * Return true iff input folder time is between compaction.timebased.min.time.ago and
   * compaction.timebased.max.time.ago.
   */
  protected boolean folderWithinAllowedPeriod(Path inputFolder, DateTime folderTime) {
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

  public static PeriodFormatter getPeriodFormatter() {
    return new PeriodFormatterBuilder().appendMonths().appendSuffix("m").appendDays().appendSuffix("d").appendHours()
        .appendSuffix("h").appendMinutes().appendSuffix("min").toFormatter();
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

  protected Path appendFolderTime(Path path, DateTime folderTime) {
    return new Path(path, folderTime.toString(this.timeFormatter));
  }
}
