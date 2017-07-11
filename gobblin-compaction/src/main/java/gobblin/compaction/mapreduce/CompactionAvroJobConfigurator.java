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

package gobblin.compaction.mapreduce;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import gobblin.compaction.mapreduce.avro.*;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.compaction.verify.InputRecordCountHelper;
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
import org.apache.commons.math3.primes.Primes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import java.util.Set;

/**
 * A configurator that focused on creating avro compaction map-reduce job
 */
@Slf4j
public class CompactionAvroJobConfigurator {
  protected final State state;

  @Getter
  protected final FileSystem fs;

  // Below attributes are MR related
  @Getter
  protected Job configuredJob;
  @Getter
  protected final boolean shouldDeduplicate;
  @Getter
  protected Path mrOutputPath = null;
  @Getter
  protected boolean isJobCreated = false;
  @Getter
  protected Collection<Path> mapReduceInputPaths = null;
  @Getter
  private long fileNameRecordCount = 0;

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

  private void configureSchema(Job job) throws IOException {
    Schema newestSchema = MRCompactorAvroKeyDedupJobRunner.getNewestSchemaFromSource(job, this.fs);
    if (this.state.getPropAsBoolean(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_SINGLE_INPUT_SCHEMA, true)) {
      AvroJob.setInputKeySchema(job, newestSchema);
    }
    AvroJob.setMapOutputKeySchema(job, this.shouldDeduplicate ? getKeySchema(job, newestSchema) : newestSchema);
    AvroJob.setMapOutputValueSchema(job, newestSchema);
    AvroJob.setOutputKeySchema(job, newestSchema);
  }

  protected void configureMapper(Job job) {
    job.setInputFormatClass(AvroKeyRecursiveCombineFileInputFormat.class);
    job.setMapperClass(AvroKeyMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
  }

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
    if (names == null || names.length == 0) {
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

    String mrOutputBase = this.state.getProp(MRCompactor.COMPACTION_JOB_DIR);
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
    Configuration conf = HadoopUtils.getConfFromState(state);

    // Turn on mapreduce output compression by default
    if (conf.get("mapreduce.output.fileoutputformat.compress") == null && conf.get("mapred.output.compress") == null) {
      conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    }

    // Disable delegation token cancellation by default
    if (conf.get("mapreduce.job.complete.cancel.delegation.tokens") == null) {
      conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    }

    addJars(conf);
    Job job = Job.getInstance(conf);
    job.setJobName(MRCompactorJobRunner.HADOOP_JOB_NAME);
    this.configureInputAndOutputPaths(job, dataset);
    this.configureMapper(job);
    this.configureReducer(job);

    if (!this.shouldDeduplicate) {
      job.setNumReduceTasks(0);
    }

    // Configure schema at the last step because FilesInputFormat will be used internally
    this.configureSchema(job);
    this.isJobCreated = true;
    this.configuredJob = job;
    return job;
  }

  private void addJars(Configuration conf) throws IOException {
    if (!state.contains(MRCompactor.COMPACTION_JARS)) {
      return;
    }
    Path jarFileDir = new Path(state.getProp(MRCompactor.COMPACTION_JARS));
    for (FileStatus status : this.fs.listStatus(jarFileDir)) {
      DistributedCache.addFileToClassPath(status.getPath(), conf, this.fs);
    }
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

    boolean appendDelta = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
            MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

    Set<Path> uncompacted = Sets.newHashSet();
    Set<Path> total = Sets.newHashSet();

    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, path)) {
      if (appendDelta) {
        // use source dir suffix to identify the delta input paths
        if (!fileStatus.getPath().getParent().toString().endsWith(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_SUFFIX)) {
          uncompacted.add(fileStatus.getPath().getParent());
        }
        total.add(fileStatus.getPath().getParent());
      } else {
        uncompacted.add(fileStatus.getPath().getParent());
      }
    }

    if (appendDelta) {
      // When the output record count from mr counter doesn't match
      // the record count from input file names, we prefer file names because
      // it will be used to calculate the difference of count in next run.
      this.fileNameRecordCount = new InputRecordCountHelper(this.state).calculateRecordCount (total);
      log.info ("{} has total input record count (based on file name) {}", path, this.fileNameRecordCount);
    }

    return uncompacted;
  }
}

