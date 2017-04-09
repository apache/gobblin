package gobblin.compaction.mapreduce;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import gobblin.compaction.mapreduce.avro.*;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.dataset.Dataset;
import gobblin.dataset.FileSystemDataset;
import gobblin.util.AvroUtils;
import gobblin.util.FileListUtils;
import gobblin.util.HadoopUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math3.primes.Primes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Arrays;
import java.util.Set;

/**
 * A configurator that focused on creating avro compaction map-reduce job
 */
@Slf4j
public class CompactionAvroJobConfigurator {
  protected final State state;
  protected final FileSystem fs;

  // Below attributes are MR related
  @Getter
  protected final boolean shouldDeduplicate;
  @Getter
  protected Path mrOutputPath = null;
  @Getter
  protected boolean isJobCreated = false;
  @Getter
  protected Collection<Path> mapReduceInputPaths = null;

  /**
   * Constructor
   * @param  state  A task level state
   */
  public CompactionAvroJobConfigurator(State state) throws IOException {
    this.state = state;
    this.fs = getFileSystem(state);
    this.shouldDeduplicate = state.getPropAsBoolean(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, true);
}

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#getNewestSchemaFromSource(Job)}
   */
  private Schema getNewestSchemaFromSource(Job job) throws IOException {
    Path[] sourceDirs = FileInputFormat.getInputPaths(job);

    List<FileStatus> files = new ArrayList<>();

    for (Path sourceDir : sourceDirs) {
      files.addAll(Arrays.asList(this.fs.listStatus(sourceDir)));
    }

    Collections.sort(files, new MRCompactorAvroKeyDedupJobRunner.LastModifiedDescComparator());

    for (FileStatus file : files) {
      Schema schema = getNewestSchemaFromSource(file.getPath());
      if (schema != null) {
        return schema;
      }
    }
    return null;
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#getNewestSchemaFromSource(Path)}
   */
  private Schema getNewestSchemaFromSource(Path sourceDir) throws IOException {
    FileStatus[] files = this.fs.listStatus(sourceDir);
    Arrays.sort(files, new MRCompactorAvroKeyDedupJobRunner.LastModifiedDescComparator());
    for (FileStatus status : files) {
      if (status.isDirectory()) {
        Schema schema = getNewestSchemaFromSource(status.getPath());
        if (schema != null)
          return schema;
      } else if (FilenameUtils.isExtension(status.getPath().getName(), "avro")) {
        return AvroUtils.getSchemaFromDataFile(status.getPath(), this.fs);
      }
    }
    return null;
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#getDedupKeyOption()}
   */
  private MRCompactorAvroKeyDedupJobRunner.DedupKeyOption getDedupKeyOption() {
    if (!this.state.contains(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_DEDUP_KEY)) {
      return MRCompactorAvroKeyDedupJobRunner.DEFAULT_DEDUP_KEY_OPTION;
    }
    Optional<MRCompactorAvroKeyDedupJobRunner.DedupKeyOption> option = Enums.getIfPresent(MRCompactorAvroKeyDedupJobRunner.DedupKeyOption.class,
            this.state.getProp(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_DEDUP_KEY).toUpperCase());
    return option.isPresent() ? option.get() : MRCompactorAvroKeyDedupJobRunner.DEFAULT_DEDUP_KEY_OPTION;
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#getKeySchema(Job, Schema)}
   */
  private Schema getKeySchema(Job job, Schema topicSchema) throws IOException {

    boolean keySchemaFileSpecified = this.state.contains(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC);

    Schema keySchema = null;

    MRCompactorAvroKeyDedupJobRunner.DedupKeyOption dedupKeyOption = getDedupKeyOption();
    if (dedupKeyOption == MRCompactorAvroKeyDedupJobRunner.DedupKeyOption.ALL) {
      log.info("Using all attributes in the schema (except Map, Arrar and Enum fields) for compaction");
      keySchema = AvroUtils.removeUncomparableFields(topicSchema).get();
    } else if (dedupKeyOption == MRCompactorAvroKeyDedupJobRunner.DedupKeyOption.KEY) {
      log.info("Using key attributes in the schema for compaction");
      keySchema = AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
    } else if (keySchemaFileSpecified) {
      Path keySchemaFile = new Path(state.getProp(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC));
      log.info("Using attributes specified in schema file " + keySchemaFile + " for compaction");
      try {
        keySchema = AvroUtils.parseSchemaFromFile(keySchemaFile, this.fs);
      } catch (IOException e) {
        log.error("Failed to parse avro schema from " + keySchemaFile
                + ", using key attributes in the schema for compaction");
        keySchema = AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
      }

      if (!MRCompactorAvroKeyDedupJobRunner.isKeySchemaValid(keySchema, topicSchema)) {
        log.warn(String.format("Key schema %s is not compatible with record schema %s.", keySchema, topicSchema)
                + "Using key attributes in the schema for compaction");
        keySchema = AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
      }
    } else {
      log.info("Property " + MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC
              + " not provided. Using key attributes in the schema for compaction");
      keySchema = AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
    }

    return keySchema;
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#configureSchema(Job)}
   */
  private void configureSchema(Job job) throws IOException {
    Schema newestSchema = getNewestSchemaFromSource(job);
    if (this.state.getPropAsBoolean(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_SINGLE_INPUT_SCHEMA, true)) {
      AvroJob.setInputKeySchema(job, newestSchema);
    }
    AvroJob.setMapOutputKeySchema(job, this.shouldDeduplicate ? getKeySchema(job, newestSchema) : newestSchema);
    AvroJob.setMapOutputValueSchema(job, newestSchema);
    AvroJob.setOutputKeySchema(job, newestSchema);
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#configureMapper(Job)}
   */
  protected void configureMapper(Job job) {
    job.setInputFormatClass(AvroKeyRecursiveCombineFileInputFormat.class);
    job.setMapperClass(AvroKeyMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#configureReducer(Job)}
   */
  protected void configureReducer(Job job) throws IOException {
    job.setOutputFormatClass(AvroKeyCompactorOutputFormat.class);
    job.setReducerClass(AvroKeyDedupReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    setNumberOfReducers(job);
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#setNumberOfReducers(Job)}
   */
  protected void setNumberOfReducers(Job job) throws IOException {

    // get input size
    long inputSize = 0;
    for (Path inputPath : this.mapReduceInputPaths) {
      inputSize += this.fs.getContentSummary(inputPath).getLength();
    }

    // get target file size
    long targetFileSize = this.state.getPropAsLong(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE,
            MRCompactorAvroKeyDedupJobRunner.DEFAULT_COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE);

    // get max reducers
    int maxNumReducers = state.getPropAsInt(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_MAX_NUM_REDUCERS,
            MRCompactorAvroKeyDedupJobRunner.DEFAULT_COMPACTION_JOB_MAX_NUM_REDUCERS);

    int numReducers = Math.min(Ints.checkedCast(inputSize / targetFileSize) + 1, maxNumReducers);

    // get use prime reducers
    boolean usePrimeReducers = state.getPropAsBoolean(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_USE_PRIME_REDUCERS,
            MRCompactorAvroKeyDedupJobRunner.DEFAULT_COMPACTION_JOB_USE_PRIME_REDUCERS);

    if (usePrimeReducers && numReducers != 1) {
      numReducers = Primes.nextPrime(numReducers);
    }
    job.setNumReduceTasks(numReducers);
  }

  /**
   * Concatenate multiple directory or file names into one path
   *
   * @return Concatenated path or null if the parameter is empty
   */
  private Path concatPaths (String ...names) {
    if (names.length == 0) {
      return null;
    }
    Path cur = new Path(names[0]);
    for (int i = 1; i < names.length; ++i) {
      cur = new Path(cur, new Path(names[i]));
    }
    return  cur;
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#configureInputAndOutputPaths(Job)}
   */
  protected void configureInputAndOutputPaths(Job job, FileSystemDataset dataset) throws IOException {

    this.mapReduceInputPaths = getGranularInputPaths(dataset.datasetRoot());
    for (Path path: mapReduceInputPaths) {
      FileInputFormat.addInputPath(job, path);
    }

    String mrOutputBase = this.state.getProp(MRCompactor.COMPACTION_TMP_DEST_DIR);
    CompactionPathParser parser = new CompactionPathParser(this.state);
    CompactionPathParser.CompactionParserResult rst = parser.parse(dataset);
    this.mrOutputPath = concatPaths (mrOutputBase, rst.getDatasetName(), rst.getDstSubDir(), rst.getTimeString());

    log.info ("Cleaning temporary MR output directory: " + mrOutputPath);
    this.fs.delete(mrOutputPath, true);
    FileOutputFormat.setOutputPath(job, mrOutputPath);
  }

  /**
   * Customized MR job creation. This method will be used in
   * {@link gobblin.compaction.suite.CompactionAvroSuite#createJob(Dataset)}
   *
   * @param  dataset  A path or directory which needs compaction
   * @return A configured map-reduce job for avro compaction
   */
  public Job createJob(FileSystemDataset dataset) throws IOException {
    Job job = Job.getInstance(new Configuration());
    this.configureInputAndOutputPaths(job, dataset);
    this.configureMapper(job);
    this.configureReducer(job);

    if (!this.shouldDeduplicate) {
      job.setNumReduceTasks(0);
    }

    // Configure schema at the last step because FilesInputFormat will be used internally
    this.configureSchema(job);
    this.isJobCreated = true;
    return job;
  }

  /**
   * Examine if a map-reduce job is already created
   * @return true if job has been created
   */
  public boolean isJobCreated() {
    return isJobCreated;
  }

  private FileSystem getFileSystem(State state)
          throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    return fs;
  }

  /**
   * Converts a top level input path to a group of sub-paths according to user defined granularity.
   * This may be required because if upstream application generates many sub-paths but the map-reduce
   * job only keeps track of the top level path, after the job is done, we won't be able to tell if
   * those new arriving sub-paths is processed by previous map-reduce job or not. Hence a better way
   * is to pre-define those sub-paths as input paths before we start to run MR. The implementation of
   * this method should depend on the data generation granularity controlled by upstream. Here we just
   * list the deepest level of containing folder as the smallest granularity.
   *
   * @param path top level directory needs compaction
   * @return A collection of input paths which will participate in map-reduce job
   */
  protected Collection<Path> getGranularInputPaths (Path path) throws IOException {

    Set<Path> output = Sets.newHashSet();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, path)) {
      output.add(fileStatus.getPath().getParent());
    }

    return output;
  }
}

