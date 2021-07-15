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

package org.apache.gobblin.compaction.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.math3.primes.Primes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.dataset.DatasetHelper;
import org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.compaction.verify.InputRecordCountHelper;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.HadoopUtils;


/**
 * Configurator for compaction job.
 * Different data formats should have their own impl. for this interface.
 *
 */
@Slf4j
public abstract class CompactionJobConfigurator {

  public static final String COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY = "compaction.jobConfiguratorFactory.class";
  public static final String DEFAULT_COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS =
      "org.apache.gobblin.compaction.mapreduce.CompactionAvroJobConfigurator$Factory";

  @Getter
  @AllArgsConstructor
  protected enum EXTENSION {
    AVRO("avro"), ORC("orc");

    private String extensionString;
  }

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
  //All the old files, which is needed when emit GMCE to register iceberg data
  @Getter
  protected Collection<String> oldFiles = null;
  //All the new files in the final publish dir, which is needed when emit GMCE to register iceberg data
  @Getter
  @Setter
  protected Collection<Path> dstNewFiles = null;
  @Getter
  protected long fileNameRecordCount = 0;

  public interface ConfiguratorFactory {
    CompactionJobConfigurator createConfigurator(State state) throws IOException;
  }

  public CompactionJobConfigurator(State state) throws IOException {
    this.state = state;
    this.fs = getFileSystem(state);
    this.shouldDeduplicate = state.getPropAsBoolean(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, true);
  }

  public static CompactionJobConfigurator instantiateConfigurator(State state) {
    String compactionConfiguratorFactoryClass =
        state.getProp(COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS_KEY, DEFAULT_COMPACTION_JOB_CONFIGURATOR_FACTORY_CLASS);
    try {
      return Class.forName(compactionConfiguratorFactoryClass)
          .asSubclass(ConfiguratorFactory.class)
          .newInstance()
          .createConfigurator(state);
    } catch (ReflectiveOperationException | IOException e) {
      throw new RuntimeException("Failed to instantiate a instance of job configurator:", e);
    }
  }

  public abstract String getFileExtension();

  /**
   * Customized MR job creation for Avro.
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

    addJars(conf, this.state, fs);
    Job job = Job.getInstance(conf);
    job.setJobName(MRCompactorJobRunner.HADOOP_JOB_NAME);
    boolean emptyDirectoryFlag = this.configureInputAndOutputPaths(job, dataset);
    if (emptyDirectoryFlag) {
      this.state.setProp(HiveRegistrationPolicy.MAPREDUCE_JOB_INPUT_PATH_EMPTY_KEY, true);
    }
    this.configureMapper(job);
    this.configureReducer(job);
    if (emptyDirectoryFlag || !this.shouldDeduplicate) {
      job.setNumReduceTasks(0);
    }
    // Configure schema at the last step because FilesInputFormat will be used internally
    this.configureSchema(job);
    this.isJobCreated = true;
    this.configuredJob = job;
    return job;
  }

  /**
   * Configuring Mapper/Reducer's input/output schema for compaction MR job.
   * The input schema for Mapper should be obtained from to-be-compacted file.
   * The output schema for Mapper is for dedup.
   * The output schema for Reducer should be identical to input schema of Mapper.
   * @param job The compaction jobConf.
   * @throws IOException
   */
  protected abstract void configureSchema(Job job) throws IOException;

  /**
   * Configuring Mapper class, specific to data format.
   */
  protected abstract void configureMapper(Job job);

  /**
   * Configuring Reducer class, specific to data format.
   */
  protected abstract void configureReducer(Job job) throws IOException;

  protected FileSystem getFileSystem(State state) throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return FileSystem.get(URI.create(uri), conf);
  }

  /**
   * Refer to {@link MRCompactorAvroKeyDedupJobRunner#setNumberOfReducers(Job)}
   * Note that this method is not format specific.
   */
  protected void setNumberOfReducers(Job job) throws IOException {

    // get input size
    long inputSize = 0;
    for (Path inputPath : this.mapReduceInputPaths) {
      inputSize += this.fs.getContentSummary(inputPath).getLength();
    }

    // get target file size
    long targetFileSize =
        this.state.getPropAsLong(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE,
            MRCompactorAvroKeyDedupJobRunner.DEFAULT_COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE);

    // get max reducers
    int maxNumReducers = state.getPropAsInt(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_MAX_NUM_REDUCERS,
        MRCompactorAvroKeyDedupJobRunner.DEFAULT_COMPACTION_JOB_MAX_NUM_REDUCERS);

    int numReducers = Math.min(Ints.checkedCast(inputSize / targetFileSize) + 1, maxNumReducers);

    // get use prime reducers
    boolean usePrimeReducers =
        state.getPropAsBoolean(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_USE_PRIME_REDUCERS,
            MRCompactorAvroKeyDedupJobRunner.DEFAULT_COMPACTION_JOB_USE_PRIME_REDUCERS);

    if (usePrimeReducers && numReducers != 1) {
      numReducers = Primes.nextPrime(numReducers);
    }
    job.setNumReduceTasks(numReducers);
  }

  protected void addJars(Configuration conf, State state, FileSystem fs) throws IOException {
    if (!state.contains(MRCompactor.COMPACTION_JARS)) {
      return;
    }
    Path jarFileDir = new Path(state.getProp(MRCompactor.COMPACTION_JARS));
    for (FileStatus status : fs.listStatus(jarFileDir)) {
      DistributedCache.addFileToClassPath(status.getPath(), conf, fs);
    }
  }

  /**
   * Refer to MRCompactorAvroKeyDedupJobRunner#configureInputAndOutputPaths(Job).
   * @return false if no valid input paths present for MR job to process,  where a path is valid if it is
   * a directory containing one or more files.
   *
   */
  protected boolean configureInputAndOutputPaths(Job job, FileSystemDataset dataset) throws IOException {
    boolean emptyDirectoryFlag = false;

    String mrOutputBase = this.state.getProp(MRCompactor.COMPACTION_JOB_DIR);
    CompactionPathParser parser = new CompactionPathParser(this.state);
    CompactionPathParser.CompactionParserResult rst = parser.parse(dataset);
    this.mrOutputPath = concatPaths(mrOutputBase, rst.getDatasetName(), rst.getDstSubDir(), rst.getTimeString());

    if(this.state.contains(ConfigurationKeys.USE_DATASET_LOCAL_WORK_DIR)) {
      mrOutputBase = this.state.getProp(MRCompactor.COMPACTION_DEST_DIR);
      this.mrOutputPath = concatPaths(mrOutputBase, rst.getDatasetName(),
          ConfigurationKeys.TMP_DIR, rst.getDstSubDir(), rst.getTimeString());
    }
    log.info("Cleaning temporary MR output directory: " + mrOutputPath);
    this.fs.delete(mrOutputPath, true);

    this.mapReduceInputPaths = getGranularInputPaths(dataset.datasetRoot());
    if (this.mapReduceInputPaths.isEmpty()) {
      this.mapReduceInputPaths.add(dataset.datasetRoot());
      emptyDirectoryFlag = true;
    }
    this.oldFiles = new HashSet<>();
    for (Path path : mapReduceInputPaths) {
      oldFiles.add(this.fs.makeQualified(path).toString());
      FileInputFormat.addInputPath(job, path);
    }

    FileOutputFormat.setOutputPath(job, mrOutputPath);
    return emptyDirectoryFlag;
  }

  /**
   * Concatenate multiple directory or file names into one path
   *
   * @return Concatenated path or null if the parameter is empty
   */
  private Path concatPaths(String... names) {
    if (names == null || names.length == 0) {
      return null;
    }
    Path cur = new Path(names[0]);
    for (int i = 1; i < names.length; ++i) {
      cur = new Path(cur, new Path(names[i]));
    }
    return cur;
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
  protected Collection<Path> getGranularInputPaths(Path path) throws IOException {

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
      this.fileNameRecordCount = new InputRecordCountHelper(this.state).calculateRecordCount(total);
      log.info("{} has total input record count (based on file name) {}", path, this.fileNameRecordCount);
    }

    return uncompacted;
  }

  private static List<TaskCompletionEvent> getAllTaskCompletionEvent(Job completedJob) {
    List<TaskCompletionEvent> completionEvents = new LinkedList<>();

    while (true) {
      try {
        TaskCompletionEvent[] bunchOfEvents;
        bunchOfEvents = completedJob.getTaskCompletionEvents(completionEvents.size());
        if (bunchOfEvents == null || bunchOfEvents.length == 0) {
          break;
        }
        completionEvents.addAll(Arrays.asList(bunchOfEvents));
      } catch (IOException e) {
        break;
      }
    }

    return completionEvents;
  }

  private static List<TaskCompletionEvent> getUnsuccessfulTaskCompletionEvent(Job completedJob) {
    return getAllTaskCompletionEvent(completedJob).stream()
        .filter(te -> te.getStatus() != TaskCompletionEvent.Status.SUCCEEDED)
        .collect(Collectors.toList());
  }

  private static boolean isFailedPath(Path path, List<TaskCompletionEvent> failedEvents) {
    return path.toString().contains("_temporary") || failedEvents.stream()
        .anyMatch(
            event -> path.toString().contains(Path.SEPARATOR + event.getTaskAttemptId().toString() + Path.SEPARATOR));
  }

  /**
   * Get good files
   * The problem happens when speculative task attempt initialized but then killed in the middle of processing.
   * Some partial file was generated at {tmp_output}/_temporary/1/_temporary/attempt_xxx_xxx/xxxx(Avro file
   * might have .avro as extension file name), without being committed to its final destination
   * at {tmp_output}/xxxx.
   *
   * @param job Completed MR job
   * @param fs File system that can handle file system
   * @param acceptableExtension file extension acceptable as "good files".
   * @return all successful files that has been committed
   */
  public static List<Path> getGoodFiles(Job job, Path tmpPath, FileSystem fs, List<String> acceptableExtension)
      throws IOException {
    List<TaskCompletionEvent> failedEvents = getUnsuccessfulTaskCompletionEvent(job);

    List<Path> allFilePaths = DatasetHelper.getApplicableFilePaths(fs, tmpPath, acceptableExtension);
    List<Path> goodPaths = new ArrayList<>();
    for (Path filePath : allFilePaths) {
      if (isFailedPath(filePath, failedEvents)) {
        fs.delete(filePath, false);
        log.error("{} is a bad path so it was deleted", filePath);
      } else {
        goodPaths.add(filePath);
      }
    }

    return goodPaths;
  }
}
