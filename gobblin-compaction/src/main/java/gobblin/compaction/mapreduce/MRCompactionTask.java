package gobblin.compaction.mapreduce;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import com.google.common.primitives.Ints;
import gobblin.compaction.dataset.CompactionParser;
import gobblin.compaction.dataset.CompactionPartition;
import gobblin.compaction.dataset.CompactionSuite;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.compaction.mapreduce.avro.AvroKeyCompactorOutputFormat;
import gobblin.compaction.mapreduce.avro.AvroKeyDedupReducer;
import gobblin.compaction.mapreduce.avro.AvroKeyMapper;
import gobblin.compaction.mapreduce.avro.AvroKeyRecursiveCombineFileInputFormat;
import gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.runtime.TaskContext;
import gobblin.runtime.mapreduce.MRTask;
import gobblin.util.AvroUtils;
import gobblin.util.FileListUtils;
import gobblin.util.HadoopUtils;


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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;


/**
 * Customized task of type {@link MRTask}, which runs MR job to compact
 * partition represented by {@link CompactionPartition}. This class replicates
 * the main logic from {@link MRCompactorAvroKeyDedupJobRunner} but share the
 * same gobblin-runtime code base. Please refer to {@link gobblin.compaction.source.CompactionSource}
 * to understand how {@link CompactionPartition} workunits are created.
 */
@Slf4j
public class MRCompactionTask extends MRTask {
  protected final CompactionPartition partition;
  protected final CompactionSuite suite;
  protected final FileSystem fs;
  protected final State state;
  // Below attributes are MR related
  protected final List<Path> mrInputPathes;
  protected final boolean shouldDeduplicate;
  protected Path mrOutput;

  /**
   * Constructor
   */
  public MRCompactionTask(TaskContext taskContext) throws IOException {
    super(taskContext);
    this.state = taskContext.getTaskState();
    this.fs = getFileSystem(this.state);
    this.partition = new CompactionPartition(this.state);
    this.suite = this.partition.getSuite();

    this.mrInputPathes = getMRInputPathsInGranularity(this.partition.getPath());
    this.shouldDeduplicate = this.state.getPropAsBoolean(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, true);
  }

  /**
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#getNewestSchemaFromSource(Job)}
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
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#getNewestSchemaFromSource(Path)}
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
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#getDedupKeyOption()}
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
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#getKeySchema(Job, Schema)}
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
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#configureSchema(Job)}
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
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#configureMapper(Job)}
   */
  protected void configureMapper(Job job) {
    job.setInputFormatClass(AvroKeyRecursiveCombineFileInputFormat.class);
    job.setMapperClass(AvroKeyMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
  }

  /**
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#configureReducer(Job)}
   */
  protected void configureReducer(Job job) throws IOException {
    job.setOutputFormatClass(AvroKeyCompactorOutputFormat.class);
    job.setReducerClass(AvroKeyDedupReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    setNumberOfReducers(job);
  }

  /**
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#setNumberOfReducers(Job)}
   */
  protected void setNumberOfReducers(Job job) throws IOException {

    // get input size
    long inputSize = 0;
    for (Path inputPath : this.mrInputPathes) {
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
   * Concatenate multiple directories into one path
   */
  private Path concatPaths (String a, String b, String c, String d) {
    return new Path (a, new Path (b, new Path (c, new Path (d))));
  }

  /**
   * mimic {@link MRCompactorAvroKeyDedupJobRunner#configureInputAndOutputPaths(Job)}
   */
  protected void configureInputAndOutputPaths(Job job) throws IOException {
    for (Path path: mrInputPathes) {
      FileInputFormat.addInputPath(job, path);
    }

    String mrOutputBase = this.state.getProp(MRCompactor.COMPACTION_TMP_DEST_DIR);

    CompactionParser.CompactionParserResult rst = this.suite.getParser().parse(this.partition);
    this.mrOutput = concatPaths (mrOutputBase, rst.getDatasetName(), rst.getDstSubDir(), rst.getTimeString());

    log.debug ("Cleanup temp MR output directory: " + mrOutput);
    this.fs.delete(mrOutput, true);
    FileOutputFormat.setOutputPath(job, mrOutput);
  }

  @Override
  public void run() {
    for (CompactionVerifier verifier: getRequiredVerifier()) {
      if (!verifier.verify(this.partition)) {
        return;
      }
    }
    super.run();
    onMRJobCompletion();
  }

  /**
   * Get all required verifiers that needs to be done before we run MR job
   */
  protected List<CompactionVerifier> getRequiredVerifier (){
    ArrayList<CompactionVerifier> verifiers = new ArrayList<>();
    verifiers.add(suite.getVerifiers().get(CompactionVerifier.COMPACTION_VERIFIER_AUDIT_COUNT));
    verifiers.add(suite.getVerifiers().get(CompactionVerifier.COMPACTION_VERIFIER_THRESHOLD));
    return verifiers;
  }

  /**
   * customized MR job creation
   */
  protected Job createJob() throws IOException {
    Job job = Job.getInstance(new Configuration());
    this.configureInputAndOutputPaths(job);
    this.configureMapper(job);
    this.configureReducer(job);

    if (!this.shouldDeduplicate) {
      job.setNumReduceTasks(0);
    }

    // Configure schema at the last step because FilesInputFormat will be used internally
    this.configureSchema(job);
    return job;
  }

  private FileSystem getFileSystem(State state)
          throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    return fs;
  }

  /**
   * Provides a granularity control of input paths. Sometimes user may want to know which level of sub-directories has participated in
   * the compaction.
   */
  protected List<Path> getMRInputPathsInGranularity (Path path) throws IOException {

    Set<Path> output = Sets.newHashSet();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, path)) {
      output.add(fileStatus.getPath().getParent());
    }

    return new LinkedList<>(output);
  }

  /**
   * A hook triggered after the compaction is done
   */
  protected void onMRJobCompletion() {
    /**
     * TODO: rename MR input directories
     * TODO: move MR output to destination
     * TODO: hive registration
     */
  }
}
