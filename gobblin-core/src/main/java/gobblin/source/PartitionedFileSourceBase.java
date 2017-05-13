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

package gobblin.source;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.filebased.FileBasedSource;
import gobblin.source.extractor.hadoop.AvroFsHelper;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.MultiWorkUnitWeightedQueue;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.DatePartitionType;
import gobblin.writer.partitioner.TimeBasedAvroWriterPartitioner;


/**
 * Implementation of {@link Source} that reads over date-partitioned files.
 * This source can be regarded as the reader equivalent of {@link TimeBasedAvroWriterPartitioner}.
 *
 * <p>
 * The class will iterate through all the data folders given by the base directory
 * {@link ConfigurationKeys#SOURCE_FILEBASED_DATA_DIRECTORY}. It relies on a {@link PartitionAwareFileRetriever}
 * to actually retrieve the files in a given partition.
 */
public abstract class PartitionedFileSourceBase<SCHEMA, DATA> extends FileBasedSource<SCHEMA, DATA> {

  // Configuration parameters
  public static final String DATE_PARTITIONED_SOURCE_PREFIX = "date.partitioned.source";

  public static final String DATE_PARTITIONED_SOURCE_PARTITION_PREFIX =
      DATE_PARTITIONED_SOURCE_PREFIX + ".partition.prefix";

  public static final String DATE_PARTITIONED_SOURCE_PARTITION_SUFFIX =
      DATE_PARTITIONED_SOURCE_PREFIX + ".partition.suffix";

  static final String DATE_PARTITIONED_SOURCE_PARTITION_PATTERN =
      DATE_PARTITIONED_SOURCE_PREFIX + ".partition.pattern";

  public static final String DATE_PARTITIONED_SOURCE_PARTITION_GRANULARITY =
      DATE_PARTITIONED_SOURCE_PREFIX + ".partition.granularity";
  public static final DatePartitionType DEFAULT_DATE_PARTITIONED_SOURCE_PARTITION_GRANULARITY =
      DatePartitionType.HOUR;

  /**
  * A String of the format defined by {@link #DATE_PARTITIONED_SOURCE_PARTITION_PATTERN} or
  * {@link #DATE_PARTITIONED_SOURCE_PARTITION_GRANULARITY}. For example for yyyy/MM/dd the
  * 2015/01/01 corresponds to January 1st, 2015. The job will start reading data from this point in time.
  * If this parameter is not specified the job will start reading data from
  * the beginning of Unix time.
  */
  private static final String DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE =
      DATE_PARTITIONED_SOURCE_PREFIX + ".min.watermark.value";

  /**
  * The maximum number of files that this job should process.
  */
  private static final String DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB =
      DATE_PARTITIONED_SOURCE_PREFIX + ".max.files.per.job";

  /**
  * The maximum number of MultiWorkUnits to create for this job. This number also corresponds to the number of
  * tasks (or if running on Hadoop, the number of map tasks) that will be launched in this job.
  */
  private static final String DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB =
      DATE_PARTITIONED_SOURCE_PREFIX + ".max.workunits.per.job";

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
  * to the epoch.
  */
  private static final int DEFAULT_DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE = 0;

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSourceBase.class);

  // Instance variables
  private SourceState sourceState;
  private FileSystem fs;
  private long lowWaterMark;
  private int maxFilesPerJob;
  private int maxWorkUnitsPerJob;
  private int fileCount;
  private TableType tableType;
  private Path sourceDir;
  private final PartitionAwareFileRetriever retriever;

  protected PartitionedFileSourceBase(PartitionAwareFileRetriever retriever) {
    this.retriever = retriever;
  }
 /**
   * Gobblin calls the {@link Source#getWorkunits(SourceState)} method after creating a {@link Source} object with a
   * blank constructor, so any custom initialization of the object needs to be done here.
   */
  protected void init(SourceState state) {
    retriever.init(state);

    try {
      initFileSystemHelper(state);
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }

    AvroFsHelper fsHelper = (AvroFsHelper) this.fsHelper;
    this.fs = fsHelper.getFileSystem();

    this.sourceState = state;

    this.lowWaterMark =
        getLowWaterMark(state.getPreviousWorkUnitStates(), state.getProp(DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE,
            String.valueOf(DEFAULT_DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE)));

    this.maxFilesPerJob = state.getPropAsInt(DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB,
        DEFAULT_DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB);

    this.maxWorkUnitsPerJob = state.getPropAsInt(DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB,
        DEFAULT_DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB);

    this.tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());

    this.fileCount = 0;

    this.sourceDir = new Path(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY));
 }


  @Override
  public void initFileSystemHelper(State state) throws FileBasedHelperException {
    this.fsHelper = new AvroFsHelper(state);
    this.fsHelper.connect();
  }

  @Override
  public abstract Extractor<SCHEMA, DATA> getExtractor(WorkUnitState state) throws IOException;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {

    DateTimeFormatter formatter = DateTimeFormat.fullDateTime();

    // Initialize all instance variables for this object
    init(state);

    LOG.info("Will pull data from " + formatter.print(this.lowWaterMark) + " until " + this.maxFilesPerJob
        + " files have been processed, or until there is no more data to consume");
    LOG.info("Creating workunits");

    // Weighted MultiWorkUnitWeightedQueue, the job will add new WorkUnits to the queue along with a weight for each
    // WorkUnit. The queue will take care of balancing the WorkUnits amongst a set number of MultiWorkUnits
    MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue = new MultiWorkUnitWeightedQueue(this.maxWorkUnitsPerJob);

    // Add failed work units from the previous execution
    addFailedWorkUnits(getPreviousWorkUnitsForRetry(this.sourceState), multiWorkUnitWeightedQueue);

    // If the file count has not exceeded maxFilesPerJob then start adding new WorkUnits to for this job
    if (this.fileCount >= this.maxFilesPerJob) {
      LOG.info(
          "The number of work units from previous job has already reached the upper limit, no more workunits will be made");
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

      LOG.info(
          "Will process file from previous workunit: " + wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));

      this.fileCount++;
    }
  }

  private Extract getExtractForFile(PartitionAwareFileRetriever.FileInfo file,
      String topicName,
      SourceState partitionState,
      Map<Long, Extract> extractMap) {
    Extract extract = extractMap.get(file.getWatermarkMsSinceEpoch());

    if (extract == null) {
      // Create an extract object for the dayPath

      extract = partitionState
          .createExtract(this.tableType, partitionState.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), topicName);

      LOG.info("Created extract: " + extract.getExtractId() + " for path " + topicName);
      extractMap.put(file.getWatermarkMsSinceEpoch(), extract);
    }

    return extract;
  }

  /**
   * Helper method to add new {@link WorkUnit}s for this job. It iterates through a date partitioned directory and
   * creates a {@link WorkUnit} for each file that needs to be processed. It then adds that {@link WorkUnit} to a
   * {@link MultiWorkUnitWeightedQueue}
   */
  private void addNewWorkUnits(MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue) {
    try {
      List<PartitionAwareFileRetriever.FileInfo> filesToPull =
          retriever.getFilesToProcess(this.lowWaterMark, this.maxFilesPerJob - this.fileCount);
      Collections.sort(filesToPull);
      String topicName = this.sourceDir.getName();

      SourceState partitionState = new SourceState();

      partitionState.addAll(this.sourceState);
      partitionState.setProp(ConfigurationKeys.SOURCE_ENTITY, topicName);
      Map<Long, Extract> extractMap = new HashMap<>();
      for (PartitionAwareFileRetriever.FileInfo file : filesToPull) {
        Extract extract = getExtractForFile(file, topicName, partitionState, extractMap);

        LOG.info("Will process file " + file.getFilePath());

        partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, file.getFilePath());
        partitionState.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, file.getWatermarkMsSinceEpoch());
        partitionState.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, file.getWatermarkMsSinceEpoch());
        partitionState.setProp(ConfigurationKeys.WORK_UNIT_DATE_PARTITION_KEY, file.getWatermarkMsSinceEpoch());

        WorkUnit singleWorkUnit = partitionState.createWorkUnit(extract);

        multiWorkUnitWeightedQueue.addWorkUnit(singleWorkUnit, file.getFileSize());

        this.fileCount++;
      }

      LOG.info("Total number of files extracted for the current run: " + filesToPull.size());
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Gets the LWM for this job runs. The new LWM is the HWM of the previous run + 1 unit (day,hour,minute..etc).
   * If there was no previous execution then it is set to the given lowWaterMark + 1 unit.
   */
  private long getLowWaterMark(Iterable<WorkUnitState> previousStates, String lowWaterMark) {

    long lowWaterMarkValue = retriever.getWatermarkFromString(lowWaterMark);

    // Find the max HWM from the previous states, this is the new current LWM
    for (WorkUnitState previousState : previousStates) {
      if (previousState.getWorkingState().equals(WorkUnitState.WorkingState.COMMITTED)) {
        long previousHighWaterMark = previousState.getWorkunit().getHighWaterMark();
        if (previousHighWaterMark > lowWaterMarkValue) {
          lowWaterMarkValue = previousHighWaterMark;
        }
      }
    }

    return lowWaterMarkValue + getRetriever().getWatermarkIncrementMs();
  }

  protected PartitionAwareFileRetriever getRetriever() {
    return retriever;
  }
}
