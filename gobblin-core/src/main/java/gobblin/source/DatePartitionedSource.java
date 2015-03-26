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
 * Implementation of {@link gobblin.source.Source} that reads over date-partitioned daily data. The watermark for each
 * run is the day that the job read data up to. It uses {@link WorkUnit#getHighWaterMark()} to get the watermark from
 * the previous workunits. The job has the option of wrapping around once it reaches a certain date. Currently, each
 * file corresponds to one work unit.
 */
public class DatePartitionedSource extends FileBasedSource<Schema, GenericRecord> {

  // Configuration parameters
  private static final String DATE_PARTITIONED_SOURCE_PREFIX = "date.partitioned.source.";
  private static final String DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE = DATE_PARTITIONED_SOURCE_PREFIX
      + "min.watermark.value";
  private static final String DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB = DATE_PARTITIONED_SOURCE_PREFIX
      + "max.files.per.job";
  private static final String DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB = DATE_PARTITIONED_SOURCE_PREFIX
      + "max.workunits.per.job";

  private static final int DEFAULT_DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB = 2000;
  private static final int DEFAULT_DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB = 500;
  private static final String DEFAULT_DATE_PARTITIONED_SOURCE_DAILY_FOLDER_NAME = "daily";

  // Joda formatters
  private static final DateTimeFormatter WATER_MARK_FORMATTER = DateTimeFormat.forPattern("yyyyMMdd");
  private static final DateTimeFormatter DAILY_FOLDER_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd");

  private static final Logger LOG = LoggerFactory.getLogger(DatePartitionedSource.class);

  private static final String AVRO_SUFFIX = ".avro";

  private FileSystem fs;

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
    return new DatePartitionedExtractor(state);
  }

  @Override
  public void initFileSystemHelper(State state) throws FileBasedHelperException {
    this.fsHelper = new HadoopFsHelper(state);
    this.fsHelper.connect();
  }

  private void init(SourceState state) {
    DateTimeZone.setDefault(DateTimeZone.forID("America/Los_Angeles"));
    try {
      initFileSystemHelper(state);
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    init(state);

    // Initialize filesystem variables
    HadoopFsHelper fsHelper = (HadoopFsHelper) this.fsHelper;
    this.fs = fsHelper.getFileSystem();

    // Get the starting date
    LOG.info("Retrieving the low water mark based on the previous execution");

    DateTime lowWaterMark = getLowWaterMark(state);

    LOG.info("The low water mark for this run is " + lowWaterMark);

    // Determine how many files to process
    int maxFilesPerJob =
        state
            .getPropAsInt(DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB, DEFAULT_DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB);

    int maxWorkUnitsPerJob =
        state.getPropAsInt(DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB,
            DEFAULT_DATE_PARTITIONED_SOURCE_MAX_WORKUNITS_PER_JOB);

    LOG.info("Will pull data from " + lowWaterMark + " until " + maxFilesPerJob + " files have been processed");

    // Create list of workunits
    LOG.info("Creating workunits");

    // Create namespace, tablename, and table type for the workunits
    TableType tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());

    // Weighted Queue that all the workunits will be added to
    MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue = new MultiWorkUnitWeightedQueue(maxWorkUnitsPerJob);

    // Add failed work units from the previous execution
    int fileCount = addPreviousWorkUnits(state, multiWorkUnitWeightedQueue);

    if (fileCount >= maxFilesPerJob) {
      LOG.info("The number of work units from previous job has already reached the upper limit, no more workunits will be made");
      return multiWorkUnitWeightedQueue.getList();
    }

    // Process all data from the lowWaterMark date until the maxFilesPerJob has been hit
    addNewWorkUnits(state, lowWaterMark, fileCount, maxFilesPerJob, tableType, multiWorkUnitWeightedQueue);

    return multiWorkUnitWeightedQueue.getList();
  }

  /**
   * Helper method to process the failed {@link WorkUnit} from the previous run and add them to the multiWorkUnitWeightedQueue
   * @param state
   * @param multiWorkUnitWeightedQueue
   * @return
   */
  private int addPreviousWorkUnits(SourceState state, MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue) {
    int fileCount = 0;

    // Add all workunits that failed in the previous run
    for (WorkUnit wu : this.getPreviousWorkUnitsForRetry(state)) {

      try {
        multiWorkUnitWeightedQueue.addWorkUnit(wu,
            this.fs.getFileStatus(new Path(wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL))).getLen());
      } catch (IOException e) {
        Throwables.propagate(e);
      }

      LOG.info("Will process file from previous workunit: "
          + wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));

      fileCount++;
    }
    return fileCount;
  }

  /**
   * Helper method to add new {@link WorkUnit}s for this job. It iterates through a date partitioned directory and creates
   * a {@link WorkUnit} for each file that needs to be processed
   * @param state
   * @param lowWaterMark
   * @param fileCount
   * @param maxFilesPerJob
   * @param tableType
   * @param multiWorkUnitWeightedQueue
   */
  private void addNewWorkUnits(SourceState state, DateTime lowWaterMark, int fileCount, int maxFilesPerJob,
      TableType tableType, MultiWorkUnitWeightedQueue multiWorkUnitWeightedQueue) {

    Path sourceDir = new Path(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY));
    DateTime currentDay = new DateTime();

    // Process all data from the lowWaterMark date until the maxFilesPerJob has been hit
    for (DateTime date = lowWaterMark; !date.isAfter(currentDay) && fileCount < maxFilesPerJob; date = date.plusDays(1)) {

      String topicName = sourceDir.getName();

      Path dayPath =
          new Path(sourceDir, DEFAULT_DATE_PARTITIONED_SOURCE_DAILY_FOLDER_NAME + Path.SEPARATOR
              + DAILY_FOLDER_FORMATTER.print(date));

      try {
        if (this.fs.exists(dayPath)) {

          // Create an extract object for the dayPath
          SourceState partitionState = new SourceState();

          partitionState.addAll(state);
          partitionState.setProp(ConfigurationKeys.SOURCE_ENTITY, topicName);

          Extract extract =
              partitionState.createExtract(tableType,
                  partitionState.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY), topicName);

          LOG.info("Created extract: " + extract.getExtractId() + " for path " + dayPath);

          // Create a workunit for each file in the folder
          for (FileStatus fileStatus : this.fs.listStatus(dayPath, getFileFilter())) {

            LOG.info("Will process file " + fileStatus.getPath());

            partitionState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, fileStatus.getPath());
            partitionState.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, WATER_MARK_FORMATTER.print(date));
            partitionState.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, WATER_MARK_FORMATTER.print(date));

            WorkUnit singleWorkUnit = partitionState.createWorkUnit(extract);

            multiWorkUnitWeightedQueue.addWorkUnit(singleWorkUnit, fileStatus.getLen());

            fileCount++;
          }
        } else {
          LOG.info("Path " + dayPath + " does not exist, skipping");
        }
      } catch (IOException e) {
        Throwables.propagate(e);
      }
    }

    LOG.info("Total number of files extracted for the current run: " + fileCount);
  }

  /**
   * Gets the LWM for this job runs; calculates it by looking at the work unit states from the previous job. It also
   * implements the wrap around logic, if the new LWM hits the wrap around date then it get set to its default value.
   * @param state the source state
   */
  private DateTime getLowWaterMark(SourceState state) {
    long lwm = state.getPropAsLong(DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE);
    List<WorkUnitState> previousStates = state.getPreviousWorkUnitStates();

    // Find the max HWM from the previous states, this is the new current LWM
    for (WorkUnitState previousState : previousStates) {
      if (previousState.getWorkingState().equals(WorkUnitState.WorkingState.COMMITTED)) {
        long previousHighWaterMark = previousState.getWorkunit().getHighWaterMark();
        if (previousHighWaterMark > lwm) {
          lwm = previousHighWaterMark;
        }
      }
    }

    return WATER_MARK_FORMATTER.parseDateTime(String.valueOf(lwm)).plusDays(1);
  }

  /**
   * This method is to filter out the .avro files that need to be processed.
   * @return the pathFilter
   */
  private PathFilter getFileFilter() {
    return new PathFilter() {
      @Override
      public boolean accept(Path path) {
        if (path.getName().endsWith(AVRO_SUFFIX)) {
          return true;
        }
        return false;
      }
    };
  }
}
