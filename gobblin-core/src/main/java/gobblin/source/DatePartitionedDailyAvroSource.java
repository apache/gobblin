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

package gobblin.source;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DatePartitionedExtractor;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.FileBasedSource;
import gobblin.source.extractor.hadoop.HadoopFsHelper;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.MultiWorkUnitWeightedQueue;
import gobblin.source.workunit.WorkUnit;


/**
 * Implementation of {@link gobblin.source.Source} that reads over date-partitioned Avro data.
 *
 * <p>
 *
 * For example, if {@link ConfigurationKeys#SOURCE_FILEBASED_DATA_DIRECTORY} is set to /my/data/, then the class assumes
 * folders following the pattern /my/data/daily/[year]/[month]/[day] are present. It will iterate through all the data
 * under these folders starting from the date specified by {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} until
 * either {@link #DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB} files have been processed, or until there is no more data
 * to process. For example, if {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} is set to 2015/01/01, then the job
 * will read from the folder /my/data/daily/2015/01/01/, /my/data/daily/2015/01/02/, /my/data/2015/01/03/ etc.
 *
 * <p>
 *
 * The class will only process data in Avro format.
 */
public class DatePartitionedDailyAvroSource extends FileBasedSource<Schema, GenericRecord> {

  // Configuration parameters
  private static final String DATE_PARTITIONED_SOURCE_PREFIX = "date.partitioned.source.";

  /**
   * A String of the format [year]/[month]/[day], for example 2015/01/01 corresponds to January 1st, 2015. The job will
   * start reading data from this point in time. If this parameter is not specified the job will start reading data from
   * the beginning of Unix time.
   */
  private static final String DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE = DATE_PARTITIONED_SOURCE_PREFIX
      + "min.watermark.value";

  /**
   * The maximum number of files that this job should process.
   */
  private static final String DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB = DATE_PARTITIONED_SOURCE_PREFIX
      + "max.files.per.job";

  /**
   * The maximum number of {@link MultiWorkUnits} to create for this job. This number also corresponds to the number of
   * tasks (or if running on Hadoop, the number of map tasks) that will be launched in this job.
   */
  private static final String DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB = DATE_PARTITIONED_SOURCE_PREFIX
      + "max.workunits.per.job";

  // Default configuration parameter values

  /**
   * Default value for {@link #DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB}
   */
  private static final int DEFAULT_DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB = 2000;

  /**
   * Default value for {@link #DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB}
   */
  private static final int DEFAULT_DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB = 500;

  /**
   * Controls the default value for {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE}. The default value will be set
   * to the 1970/01/01.
   */
  private static final int DEFAULT_DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE = 0;

  // String constants
  private static final String DAILY_FOLDER_NAME = "daily";
  private static final String AVRO_SUFFIX = ".avro";

  // Joda formatters
  private static final DateTimeFormatter DAILY_FOLDER_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd");

  private static final Logger LOG = LoggerFactory.getLogger(DatePartitionedDailyAvroSource.class);

  // Instance variables
  private SourceState sourceState;
  private FileSystem fs;
  private long lowWaterMark;
  private int maxFilesPerJob;
  private int maxWorkUnitsPerJob;
  private int fileCount;
  private TableType tableType;
  private Path sourceDir;

  /**
   * Gobblin calls the {@link Source#getWorkunits(SourceState)} method after creating a {@link Source} object with a
   * blank constructor, so any custom initialization of the object needs to be done here.
   */
  private void init(SourceState state) {
    DateTimeZone.setDefault(DateTimeZone.forID(state.getProp(ConfigurationKeys.SOURCE_TIMEZONE,
        ConfigurationKeys.DEFAULT_SOURCE_TIMEZONE)));

    try {
      initFileSystemHelper(state);
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }

    HadoopFsHelper fsHelper = (HadoopFsHelper) this.fsHelper;
    this.fs = fsHelper.getFileSystem();

    this.sourceState = state;

    this.lowWaterMark =
        getLowWaterMark(
            state.getPreviousWorkUnitStates(),
            state.getProp(DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE,
                DAILY_FOLDER_FORMATTER.print(DEFAULT_DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE)));

    this.maxFilesPerJob =
        state
            .getPropAsInt(DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB, DEFAULT_DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB);

    this.maxWorkUnitsPerJob =
        state.getPropAsInt(DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB,
            DEFAULT_DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB);

    this.tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());

    this.fileCount = 0;

    this.sourceDir = new Path(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY));
  }

  @Override
  public void initFileSystemHelper(State state) throws FileBasedHelperException {
    this.fsHelper = new HadoopFsHelper(state);
    this.fsHelper.connect();
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
    return new DatePartitionedExtractor(state);
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {

    // Initialize all instance variables for this object
    init(state);

    LOG.info("Will pull data from " + DAILY_FOLDER_FORMATTER.print(this.lowWaterMark) + " until " + this.maxFilesPerJob
        + " files have been processed, or until there is no more data to consume");
    LOG.info("Creating workunits");

    // Weighted MultiWorkUnitWeightedQueue, the job will add new WorkUnits to the queue along with a weight for each
    // WorkUnit. The queue will take care of balancing the WorkUnits amongst a set number of MultiWorkUnits
    MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue = new MultiWorkUnitWeightedQueue(this.maxWorkUnitsPerJob);

    // Add failed work units from the previous execution
    addFailedWorkUnits(getPreviousWorkUnitsForRetry(this.sourceState), multiWorkUnitWeightedQueue);

    // If the file count has not exceeded maxFilesPerJob then start adding new WorkUnits to for this job
    if (this.fileCount >= this.maxFilesPerJob) {
      LOG.info("The number of work units from previous job has already reached the upper limit, no more workunits will be made");
      return multiWorkUnitWeightedQueue.getQueueAsList();
    }

    addNewWorkUnits(multiWorkUnitWeightedQueue);

    return multiWorkUnitWeightedQueue.getQueueAsList();
  }

  /**
   * Helper method to process the failed {@link WorkUnit}s from the previous run and add them to the a
   * {@link MultiWorkUnitWeightedQueue}
   */
  private void addFailedWorkUnits(List<WorkUnit> previousWorkUnitsForRetry,
      MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue) {
    for (WorkUnit wu : previousWorkUnitsForRetry) {

      try {
        multiWorkUnitWeightedQueue.addWorkUnit(wu,
            this.fs.getFileStatus(new Path(wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL))).getLen());
      } catch (IOException e) {
        Throwables.propagate(e);
      }

      LOG.info("Will process file from previous workunit: "
          + wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));

      this.fileCount++;
    }
  }

  /**
   * Helper method to add new {@link WorkUnit}s for this job. It iterates through a date partitioned directory and
   * creates a {@link WorkUnit} for each file that needs to be processed. It then adds that {@link WorkUnit} to a
   * {@link MultiWorkUnitWeightedQueue}
   */
  private void addNewWorkUnits(MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue) {

    DateTime currentDay = new DateTime();
    DateTime lowWaterMarkDate = new DateTime(this.lowWaterMark);
    String topicName = this.sourceDir.getName();

    // Process all data from the lowWaterMark date until the maxFilesPerJob has been hit
    for (DateTime date = lowWaterMarkDate; !date.isAfter(currentDay) && this.fileCount < this.maxFilesPerJob; date =
        date.plusDays(1)) {

      // Construct a daily path folder - e.g. /my/data/daily/2015/01/01/
      Path dayPath = new Path(this.sourceDir, DAILY_FOLDER_NAME + Path.SEPARATOR + DAILY_FOLDER_FORMATTER.print(date));

      try {
        if (this.fs.exists(dayPath)) {

          // Create an extract object for the dayPath
          SourceState partitionState = new SourceState();

          partitionState.addAll(this.sourceState);
          partitionState.setProp(ConfigurationKeys.SOURCE_ENTITY, topicName);

          Extract extract =
              partitionState.createExtract(this.tableType,
                  partitionState.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), topicName);

          LOG.info("Created extract: " + extract.getExtractId() + " for path " + dayPath);

          // Create a WorkUnit for each file in the folder
          for (FileStatus fileStatus : this.fs.listStatus(dayPath, getFileFilter())) {

            LOG.info("Will process file " + fileStatus.getPath());

            partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, fileStatus.getPath());
            partitionState.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, date.getMillis());
            partitionState.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, date.getMillis());

            WorkUnit singleWorkUnit = partitionState.createWorkUnit(extract);

            multiWorkUnitWeightedQueue.addWorkUnit(singleWorkUnit, fileStatus.getLen());

            this.fileCount++;
          }
        } else {
          LOG.info("Path " + dayPath + " does not exist, skipping");
        }
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }

    LOG.info("Total number of files extracted for the current run: " + this.fileCount);
  }

  /**
   * Gets the LWM for this job runs. The new LWM is the HWM of the previous run + 1 day. If there was no previous
   * execution then it is set to the given lowWaterMark + 1 day.
   */
  private long getLowWaterMark(List<WorkUnitState> previousStates, String lowWaterMark) {

    long lowWaterMarkValue = DAILY_FOLDER_FORMATTER.parseMillis(lowWaterMark);

    // Find the max HWM from the previous states, this is the new current LWM
    for (WorkUnitState previousState : previousStates) {
      if (previousState.getWorkingState().equals(WorkUnitState.WorkingState.COMMITTED)) {
        long previousHighWaterMark = previousState.getWorkunit().getHighWaterMark();
        if (previousHighWaterMark > lowWaterMarkValue) {
          lowWaterMarkValue = previousHighWaterMark;
        }
      }
    }

    return new DateTime(lowWaterMarkValue).plusDays(1).getMillis();
  }

  /**
   * This method is to filter out the .avro files that need to be processed.
   * @return the pathFilter
   */
  private PathFilter getFileFilter() {
    return new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(AVRO_SUFFIX);
      }
    };
  }
}
