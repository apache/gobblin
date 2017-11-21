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

package org.apache.gobblin.runtime.mapreduce;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.DynamicConfigGeneratorFactory;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.util.JobMetrics;
import org.apache.gobblin.runtime.util.MetricGroup;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobConfigurationUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.SerializationUtils;


/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job as a Hadoop MR job.
 *
 * <p>
 *     The basic idea of this implementation is to use mappers as containers to run tasks.
 *     In the Hadoop MP job, each mapper is responsible for executing one or more tasks.
 *     A mapper uses its input to get the paths of the files storing serialized work units,
 *     deserializes the work units and creates tasks, and executes the tasks in a thread
 *     pool. {@link TaskExecutor} and {@link Task} remain the same as in local single-node
 *     mode. Each mapper writes out task states upon task completion.
 * </p>
 *
 * @author Yinan Li
 */
public class MRJobLauncher extends AbstractJobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(MRJobLauncher.class);

  private static final String JOB_NAME_PREFIX = "Gobblin-";

  private static final String JARS_DIR_NAME = "_jars";
  private static final String FILES_DIR_NAME = "_files";
  static final String INPUT_DIR_NAME = "input";
  private static final String OUTPUT_DIR_NAME = "output";
  private static final String WORK_UNIT_LIST_FILE_EXTENSION = ".wulist";

  // Configuration that make uploading of jar files more reliable,
  // since multiple Gobblin Jobs are sharing the same jar directory.
  private static final int MAXIMUM_JAR_COPY_RETRY_TIMES_DEFAULT = 5;
  private static final int WAITING_TIME_ON_IMCOMPLETE_UPLOAD = 3000;

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final Configuration conf;
  private final FileSystem fs;
  private final Job job;
  private final Path mrJobDir;
  private final Path jarsDir;
  /** A location to store jars that should not be shared between different jobs. */
  private final Path unsharedJarsDir;
  private final Path jobInputPath;
  private final Path jobOutputPath;

  private final int parallelRunnerThreads;

  private final TaskStateCollectorService taskStateCollectorService;

  private volatile boolean hadoopJobSubmitted = false;

  private final StateStore<TaskState> taskStateStore;

  private final int jarFileMaximumRetry;

  public MRJobLauncher(Properties jobProps) throws Exception {
    this(jobProps, null);
  }

  public MRJobLauncher(Properties jobProps, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) throws Exception {
    this(jobProps, new Configuration(), instanceBroker);
  }

  public MRJobLauncher(Properties jobProps, Configuration conf, SharedResourcesBroker<GobblinScopeTypes> instanceBroker)
      throws Exception {
    super(jobProps, ImmutableList.<Tag<?>>of());

    this.conf = conf;
    // Put job configuration properties into the Hadoop configuration so they are available in the mappers
    JobConfigurationUtils.putPropertiesIntoConfiguration(this.jobProps, this.conf);

    // Let the job and all mappers finish even if some mappers fail
    this.conf.set("mapreduce.map.failures.maxpercent", "100"); // For Hadoop 2.x

    // Do not cancel delegation tokens after job has completed (HADOOP-7002)
    this.conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

    this.fs = buildFileSystem(jobProps, this.conf);

    this.mrJobDir = new Path(
        new Path(this.jobProps.getProperty(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY), this.jobContext.getJobName()),
        this.jobContext.getJobId());
    if (this.fs.exists(this.mrJobDir)) {
      LOG.warn("Job working directory already exists for job " + this.jobContext.getJobName());
      this.fs.delete(this.mrJobDir, true);
    }
    this.unsharedJarsDir = new Path(this.mrJobDir, JARS_DIR_NAME);
    this.jarsDir = this.jobProps.containsKey(ConfigurationKeys.MR_JARS_DIR) ? new Path(
        this.jobProps.getProperty(ConfigurationKeys.MR_JARS_DIR)) : this.unsharedJarsDir;
    this.fs.mkdirs(this.mrJobDir);

    this.jobInputPath = new Path(this.mrJobDir, INPUT_DIR_NAME);
    this.jobOutputPath = new Path(this.mrJobDir, OUTPUT_DIR_NAME);
    Path outputTaskStateDir = new Path(this.jobOutputPath, this.jobContext.getJobId());

    // Finally create the Hadoop job after all updates to conf are already made (including
    // adding dependent jars/files to the DistributedCache that also updates the conf)
    this.job = Job.getInstance(this.conf, JOB_NAME_PREFIX + this.jobContext.getJobName());

    this.parallelRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));

    // StateStore interface uses the following key (rootDir, storeName, tableName)
    // The state store base is the root directory and the last two elements of the path are used as the storeName and
    // tableName. Create the state store with the root at jobOutputPath. The task state will be stored at
    // jobOutputPath/output/taskState.tst, so output will be the storeName.
    taskStateStore = new FsStateStore<>(this.fs, jobOutputPath.toString(), TaskState.class);

    this.taskStateCollectorService =
        new TaskStateCollectorService(jobProps, this.jobContext.getJobState(), this.eventBus, taskStateStore,
            outputTaskStateDir);

    this.jarFileMaximumRetry =
        jobProps.containsKey(ConfigurationKeys.MAXIMUM_JAR_COPY_RETRY_TIMES_KEY) ? Integer.parseInt(
            jobProps.getProperty(ConfigurationKeys.MAXIMUM_JAR_COPY_RETRY_TIMES_KEY))
            : MAXIMUM_JAR_COPY_RETRY_TIMES_DEFAULT;

    startCancellationExecutor();
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.hadoopJobSubmitted && !this.job.isComplete()) {
        LOG.info("Killing the Hadoop MR job for job " + this.jobContext.getJobId());
        this.job.killJob();
      }
    } finally {
      try {
        cleanUpWorkingDirectory();
      } finally {
        super.close();
        fs.close();
      }
    }
  }

  @Override
  protected void runWorkUnits(List<WorkUnit> workUnits) throws Exception {
    String jobName = this.jobContext.getJobName();
    JobState jobState = this.jobContext.getJobState();

    try {
      prepareHadoopJob(workUnits);

      // Start the output TaskState collector service
      this.taskStateCollectorService.startAsync().awaitRunning();

      LOG.info("Launching Hadoop MR job " + this.job.getJobName());
      this.job.submit();
      this.hadoopJobSubmitted = true;

      // Set job tracking URL to the Hadoop job tracking URL if it is not set yet
      if (!jobState.contains(ConfigurationKeys.JOB_TRACKING_URL_KEY)) {
        jobState.setProp(ConfigurationKeys.JOB_TRACKING_URL_KEY, this.job.getTrackingURL());
      }

      TimingEvent mrJobRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.MR_JOB_RUN);
      LOG.info(String.format("Waiting for Hadoop MR job %s to complete", this.job.getJobID()));
      this.job.waitForCompletion(true);
      mrJobRunTimer.stop(ImmutableMap.of("hadoopMRJobId", this.job.getJobID().toString()));

      if (this.cancellationRequested) {
        // Wait for the cancellation execution if it has been requested
        synchronized (this.cancellationExecution) {
          if (this.cancellationExecuted) {
            return;
          }
        }
      }

      // Create a metrics set for this job run from the Hadoop counters.
      // The metrics set is to be persisted to the metrics store later.
      countersToMetrics(JobMetrics.get(jobName, this.jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY)));
    } finally {
      // The last iteration of output TaskState collecting will run when the collector service gets stopped
      this.taskStateCollectorService.stopAsync().awaitTerminated();
      cleanUpWorkingDirectory();
    }
  }

  @Override
  protected void executeCancellation() {
    try {
      if (this.hadoopJobSubmitted && !this.job.isComplete()) {
        LOG.info("Killing the Hadoop MR job for job " + this.jobContext.getJobId());
        this.job.killJob();
        // Collect final task states.
        this.taskStateCollectorService.stopAsync().awaitTerminated();
      }
    } catch (IllegalStateException ise) {
      LOG.error("The Hadoop MR job has not started for job " + this.jobContext.getJobId());
    } catch (IOException ioe) {
      LOG.error("Failed to kill the Hadoop MR job for job " + this.jobContext.getJobId());
    }
  }

  /**
   * Add dependent jars and files.
   */
  private void addDependencies(Configuration conf) throws IOException {
    TimingEvent distributedCacheSetupTimer =
        this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.MR_DISTRIBUTED_CACHE_SETUP);

    Path jarFileDir = this.jarsDir;

    // Add framework jars to the classpath for the mappers/reducer
    if (this.jobProps.containsKey(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY)) {
      addJars(jarFileDir, this.jobProps.getProperty(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY), conf);
    }

    // Add job-specific jars to the classpath for the mappers
    if (this.jobProps.containsKey(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      addJars(jarFileDir, this.jobProps.getProperty(ConfigurationKeys.JOB_JAR_FILES_KEY), conf);
    }

    // Add other files (if any) the job depends on to DistributedCache
    if (this.jobProps.containsKey(ConfigurationKeys.JOB_LOCAL_FILES_KEY)) {
      addLocalFiles(new Path(this.mrJobDir, FILES_DIR_NAME),
          this.jobProps.getProperty(ConfigurationKeys.JOB_LOCAL_FILES_KEY), conf);
    }

    // Add files (if any) already on HDFS that the job depends on to DistributedCache
    if (this.jobProps.containsKey(ConfigurationKeys.JOB_HDFS_FILES_KEY)) {
      addHDFSFiles(this.jobProps.getProperty(ConfigurationKeys.JOB_HDFS_FILES_KEY), conf);
    }

    // Add job-specific jars existing in HDFS to the classpath for the mappers
    if (this.jobProps.containsKey(ConfigurationKeys.JOB_JAR_HDFS_FILES_KEY)) {
      addHdfsJars(this.jobProps.getProperty(ConfigurationKeys.JOB_JAR_HDFS_FILES_KEY), conf);
    }

    distributedCacheSetupTimer.stop();
  }

  /**
   * Prepare the Hadoop MR job, including configuring the job and setting up the input/output paths.
   */
  private void prepareHadoopJob(List<WorkUnit> workUnits) throws IOException {
    TimingEvent mrJobSetupTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.MR_JOB_SETUP);

    // Add dependent jars/files
    addDependencies(this.job.getConfiguration());

    this.job.setJarByClass(MRJobLauncher.class);
    this.job.setMapperClass(TaskRunner.class);

    // The job is mapper-only
    this.job.setNumReduceTasks(0);

    this.job.setInputFormatClass(GobblinWorkUnitsInputFormat.class);
    this.job.setOutputFormatClass(GobblinOutputFormat.class);
    this.job.setMapOutputKeyClass(NullWritable.class);
    this.job.setMapOutputValueClass(NullWritable.class);

    // Set speculative execution

    this.job.setSpeculativeExecution(isSpeculativeExecutionEnabled(this.jobProps));

    this.job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

    // Job input path is where input work unit files are stored

    // Prepare job input
    prepareJobInput(workUnits);
    FileInputFormat.addInputPath(this.job, this.jobInputPath);

    // Job output path is where serialized task states are stored
    FileOutputFormat.setOutputPath(this.job, this.jobOutputPath);

    // Serialize source state to a file which will be picked up by the mappers
    serializeJobState(this.fs, this.mrJobDir, this.conf, this.jobContext.getJobState(), this.job);

    if (this.jobProps.containsKey(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY)) {
      GobblinWorkUnitsInputFormat.setMaxMappers(this.job,
          Integer.parseInt(this.jobProps.getProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY)));
    }

    mrJobSetupTimer.stop();
  }

  static boolean isSpeculativeExecutionEnabled(Properties props) {
    return Boolean.valueOf(
        props.getProperty(JobContext.MAP_SPECULATIVE, ConfigurationKeys.DEFAULT_ENABLE_MR_SPECULATIVE_EXECUTION));
  }

  @VisibleForTesting
  static void serializeJobState(FileSystem fs, Path mrJobDir, Configuration conf, JobState jobState, Job job)
      throws IOException {
    Path jobStateFilePath = new Path(mrJobDir, JOB_STATE_FILE_NAME);
    // Write the job state with an empty task set (work units are read by the mapper from a different file)
    try (DataOutputStream dataOutputStream = new DataOutputStream(fs.create(jobStateFilePath))) {
      jobState.write(dataOutputStream, false);
    }

    job.getConfiguration().set(ConfigurationKeys.JOB_STATE_FILE_PATH_KEY, jobStateFilePath.toString());

    DistributedCache.addCacheFile(jobStateFilePath.toUri(), job.getConfiguration());
    job.getConfiguration().set(ConfigurationKeys.JOB_STATE_DISTRIBUTED_CACHE_NAME, jobStateFilePath.getName());
  }

  /**
   * Add framework or job-specific jars to the classpath through DistributedCache
   * so the mappers can use them.
   */
  @SuppressWarnings("deprecation")
  private void addJars(Path jarFileDir, String jarFileList, Configuration conf) throws IOException {
    LocalFileSystem lfs = FileSystem.getLocal(conf);
    for (String jarFile : SPLITTER.split(jarFileList)) {
      Path srcJarFile = new Path(jarFile);
      FileStatus[] fileStatusList = lfs.globStatus(srcJarFile);

      for (FileStatus status : fileStatusList) {
        // For each FileStatus there are chances it could fail in copying at the first attempt, due to file-existence
        // or file-copy is ongoing by other job instance since all Gobblin jobs share the same jar file directory.
        // the retryCount is to avoid cases (if any) where retry is going too far and causes job hanging.
        int retryCount = 0;
        boolean shouldFileBeAddedIntoDC = true;
        Path destJarFile = calculateDestJarFile(status, jarFileDir);
        // Adding destJarFile into HDFS until it exists and the size of file on targetPath matches the one on local path.
        while (!this.fs.exists(destJarFile) || fs.getFileStatus(destJarFile).getLen() != status.getLen()) {
          try {
            if (this.fs.exists(destJarFile) && fs.getFileStatus(destJarFile).getLen() != status.getLen()) {
              Thread.sleep(WAITING_TIME_ON_IMCOMPLETE_UPLOAD);
              throw new IOException("Waiting for file to complete on uploading ... ");
            }
            // Set the first parameter as false for not deleting sourceFile
            // Set the second parameter as false for not overwriting existing file on the target, by default it is true.
            // If the file is preExisted but overwrite flag set to false, then an IOException if thrown.
            this.fs.copyFromLocalFile(false, false, status.getPath(), destJarFile);
          } catch (IOException | InterruptedException e) {
            LOG.warn("Path:" + destJarFile + " is not copied successfully. Will require retry.");
            retryCount += 1;
            if (retryCount >= this.jarFileMaximumRetry) {
              LOG.error("The jar file:" + destJarFile + "failed in being copied into hdfs", e);
              // If retry reaches upper limit, skip copying this file.
              shouldFileBeAddedIntoDC = false;
              break;
            }
          }
        }
        if (shouldFileBeAddedIntoDC) {
          // Then add the jar file on HDFS to the classpath
          LOG.info(String.format("Adding %s to classpath", destJarFile));
          DistributedCache.addFileToClassPath(destJarFile, conf, this.fs);
        }
      }
    }
  }

  /**
   * Calculate the target filePath of the jar file to be copied on HDFS,
   * given the {@link FileStatus} of a jarFile and the path of directory that contains jar.
   */
  private Path calculateDestJarFile(FileStatus status, Path jarFileDir) {
    // SNAPSHOT jars should not be shared, as different jobs may be using different versions of it
    Path baseDir = status.getPath().getName().contains("SNAPSHOT") ? this.unsharedJarsDir : jarFileDir;
    // DistributedCache requires absolute path, so we need to use makeQualified.
    return new Path(this.fs.makeQualified(baseDir), status.getPath().getName());
  }

  /**
   * Add local non-jar files the job depends on to DistributedCache.
   */
  @SuppressWarnings("deprecation")
  private void addLocalFiles(Path jobFileDir, String jobFileList, Configuration conf) throws IOException {
    DistributedCache.createSymlink(conf);
    for (String jobFile : SPLITTER.split(jobFileList)) {
      Path srcJobFile = new Path(jobFile);
      // DistributedCache requires absolute path, so we need to use makeQualified.
      Path destJobFile = new Path(this.fs.makeQualified(jobFileDir), srcJobFile.getName());
      // Copy the file from local file system to HDFS
      this.fs.copyFromLocalFile(srcJobFile, destJobFile);
      // Create a URI that is in the form path#symlink
      URI destFileUri = URI.create(destJobFile.toUri().getPath() + "#" + destJobFile.getName());
      LOG.info(String.format("Adding %s to DistributedCache", destFileUri));
      // Finally add the file to DistributedCache with a symlink named after the file name
      DistributedCache.addCacheFile(destFileUri, conf);
    }
  }

  /**
   * Add non-jar files already on HDFS that the job depends on to DistributedCache.
   */
  @SuppressWarnings("deprecation")
  private void addHDFSFiles(String jobFileList, Configuration conf) {
    DistributedCache.createSymlink(conf);
    jobFileList = PasswordManager.getInstance(this.jobProps).readPassword(jobFileList);
    for (String jobFile : SPLITTER.split(jobFileList)) {
      Path srcJobFile = new Path(jobFile);
      // Create a URI that is in the form path#symlink
      URI srcFileUri = URI.create(srcJobFile.toUri().getPath() + "#" + srcJobFile.getName());
      LOG.info(String.format("Adding %s to DistributedCache", srcFileUri));
      // Finally add the file to DistributedCache with a symlink named after the file name
      DistributedCache.addCacheFile(srcFileUri, conf);
    }
  }

  private void addHdfsJars(String hdfsJarFileList, Configuration conf) throws IOException {
    for (String jarFile : SPLITTER.split(hdfsJarFileList)) {
      FileStatus[] status = this.fs.listStatus(new Path(jarFile));
      for (FileStatus fileStatus : status) {
        if (!fileStatus.isDirectory()) {
          Path path = new Path(jarFile, fileStatus.getPath().getName());
          LOG.info(String.format("Adding %s to classpath", path));
          DistributedCache.addFileToClassPath(path, conf, this.fs);
        }
      }
    }
  }

  /**
   * Prepare the job input.
   * @throws IOException
   */
  private void prepareJobInput(List<WorkUnit> workUnits) throws IOException {
    Closer closer = Closer.create();
    try {
      ParallelRunner parallelRunner = closer.register(new ParallelRunner(this.parallelRunnerThreads, this.fs));

      int multiTaskIdSequence = 0;
      // Serialize each work unit into a file named after the task ID
      for (WorkUnit workUnit : workUnits) {

        String workUnitFileName;
        if (workUnit instanceof MultiWorkUnit) {
          workUnitFileName = JobLauncherUtils.newMultiTaskId(this.jobContext.getJobId(), multiTaskIdSequence++)
              + MULTI_WORK_UNIT_FILE_EXTENSION;
        } else {
          workUnitFileName = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY) + WORK_UNIT_FILE_EXTENSION;
        }
        Path workUnitFile = new Path(this.jobInputPath, workUnitFileName);
        LOG.debug("Writing work unit file " + workUnitFileName);

        parallelRunner.serializeToFile(workUnit, workUnitFile);

        // Append the work unit file path to the job input file
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Cleanup the Hadoop MR working directory.
   */
  private void cleanUpWorkingDirectory() {
    try {
      if (this.fs.exists(this.mrJobDir)) {
        this.fs.delete(this.mrJobDir, true);
        LOG.info("Deleted working directory " + this.mrJobDir);
      }
    } catch (IOException ioe) {
      LOG.error("Failed to delete working directory " + this.mrJobDir);
    }
  }

  /**
   * Create a {@link org.apache.gobblin.metrics.GobblinMetrics} instance for this job run from the Hadoop counters.
   */
  @VisibleForTesting
  void countersToMetrics(GobblinMetrics metrics) throws IOException {
    Optional<Counters> counters = Optional.fromNullable(this.job.getCounters());

    if (counters.isPresent()) {
      // Write job-level counters
      CounterGroup jobCounterGroup = counters.get().getGroup(MetricGroup.JOB.name());
      for (Counter jobCounter : jobCounterGroup) {
        metrics.getCounter(jobCounter.getName()).inc(jobCounter.getValue());
      }

      // Write task-level counters
      CounterGroup taskCounterGroup = counters.get().getGroup(MetricGroup.TASK.name());
      for (Counter taskCounter : taskCounterGroup) {
        metrics.getCounter(taskCounter.getName()).inc(taskCounter.getValue());
      }
    }
  }

  private static FileSystem buildFileSystem(Properties jobProps, Configuration configuration) throws IOException {
    URI fsUri = URI.create(jobProps.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    return FileSystem.newInstance(fsUri, configuration);
  }

  /**
   * The mapper class that runs assigned {@link WorkUnit}s.
   *
   * <p>
   *   The {@link #map} method de-serializes a {@link WorkUnit} (maybe a {@link MultiWorkUnit})
   *   from each input file and add the {@link WorkUnit} (or a list of {@link WorkUnit}s if it
   *   is a {@link MultiWorkUnit} to the list of {@link WorkUnit}s to run. The {@link #run} method
   *   actually runs the list of {@link WorkUnit}s in the {@link TaskExecutor}. This allows the
   *   {@link WorkUnit}s to be run in parallel if the {@link TaskExecutor} is configured to have
   *   more than one thread in its thread pool.
   * </p>
   */
  public static class TaskRunner extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private FileSystem fs;
    private StateStore<TaskState> taskStateStore;
    private TaskExecutor taskExecutor;
    private TaskStateTracker taskStateTracker;
    private ServiceManager serviceManager;
    private Optional<JobMetrics> jobMetrics = Optional.absent();
    private boolean isSpeculativeEnabled;
    private final JobState jobState = new JobState();

    // A list of WorkUnits (flattened for MultiWorkUnits) to be run by this mapper
    private final List<WorkUnit> workUnits = Lists.newArrayList();

    @Override
    protected void setup(Context context) {
      try (Closer closer = Closer.create()) {
        this.isSpeculativeEnabled =
            isSpeculativeExecutionEnabled(HadoopUtils.getStateFromConf(context.getConfiguration()).getProperties());
        this.fs = FileSystem.get(context.getConfiguration());
        this.taskStateStore =
            new FsStateStore<>(this.fs, FileOutputFormat.getOutputPath(context).toUri().getPath(), TaskState.class);

        String jobStateFileName = context.getConfiguration().get(ConfigurationKeys.JOB_STATE_DISTRIBUTED_CACHE_NAME);
        boolean foundStateFile = false;
        for (Path dcPath : DistributedCache.getLocalCacheFiles(context.getConfiguration())) {
          if (dcPath.getName().equals(jobStateFileName)) {
            SerializationUtils.deserializeStateFromInputStream(
                closer.register(new FileInputStream(dcPath.toUri().getPath())), this.jobState);
            foundStateFile = true;
            break;
          }
        }
        if (!foundStateFile) {
          throw new IOException("Job state file not found.");
        }
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to setup the mapper task", ioe);
      }


      // load dynamic configuration to add to the job configuration
      Configuration configuration = context.getConfiguration();
      Config jobStateAsConfig = ConfigUtils.propertiesToConfig(this.jobState.getProperties());
      DynamicConfigGenerator dynamicConfigGenerator = DynamicConfigGeneratorFactory.createDynamicConfigGenerator(
          jobStateAsConfig);
      Config dynamicConfig = dynamicConfigGenerator.generateDynamicConfig(jobStateAsConfig);

      // add the dynamic config to the job config
      for (Map.Entry<String, ConfigValue> entry : dynamicConfig.entrySet()) {
        this.jobState.setProp(entry.getKey(), entry.getValue().unwrapped().toString());
        configuration.set(entry.getKey(), entry.getValue().unwrapped().toString());
      }

      this.taskExecutor = new TaskExecutor(configuration);
      this.taskStateTracker = new MRTaskStateTracker(context);
      this.serviceManager = new ServiceManager(Lists.newArrayList(this.taskExecutor, this.taskStateTracker));
      try {
        this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        LOG.error("Timed out while waiting for the service manager to start up", te);
        throw new RuntimeException(te);
      }

      // Setup and start metrics reporting if metric reporting is enabled
      if (Boolean.valueOf(
          configuration.get(ConfigurationKeys.METRICS_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_ENABLED))) {
        this.jobMetrics = Optional.of(JobMetrics.get(this.jobState));
        this.jobMetrics.get()
            .startMetricReportingWithFileSuffix(HadoopUtils.getStateFromConf(configuration),
                context.getTaskAttemptID().toString());
      }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      this.setup(context);
      GobblinMultiTaskAttempt gobblinMultiTaskAttempt = null;
      try {
        // De-serialize and collect the list of WorkUnits to run
        while (context.nextKeyValue()) {
          this.map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        GobblinMultiTaskAttempt.CommitPolicy multiTaskAttemptCommitPolicy =
            isSpeculativeEnabled ? GobblinMultiTaskAttempt.CommitPolicy.CUSTOMIZED
                : GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE;

        SharedResourcesBroker<GobblinScopeTypes> globalBroker =
            SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
                ConfigFactory.parseProperties(this.jobState.getProperties()),
                GobblinScopeTypes.GLOBAL.defaultScopeInstance());
        SharedResourcesBroker<GobblinScopeTypes> jobBroker =
            globalBroker.newSubscopedBuilder(new JobScopeInstance(this.jobState.getJobName(), this.jobState.getJobId()))
                .build();

        // Actually run the list of WorkUnits
        gobblinMultiTaskAttempt =
            GobblinMultiTaskAttempt.runWorkUnits(this.jobState.getJobId(), context.getTaskAttemptID().toString(),
                this.jobState, this.workUnits, this.taskStateTracker, this.taskExecutor, this.taskStateStore,
                multiTaskAttemptCommitPolicy, jobBroker);

        if (this.isSpeculativeEnabled) {
          LOG.info("will not commit in task attempt");
          GobblinOutputCommitter gobblinOutputCommitter = (GobblinOutputCommitter) context.getOutputCommitter();
          gobblinOutputCommitter.getAttemptIdToMultiTaskAttempt()
              .put(context.getTaskAttemptID().toString(), gobblinMultiTaskAttempt);
        }
      } finally {
        CommitStep cleanUpCommitStep = new CommitStep() {

          @Override
          public boolean isCompleted() throws IOException {
            return !serviceManager.isHealthy();
          }

          @Override
          public void execute() throws IOException {
            LOG.info("Starting the clean-up steps.");
            try {
              serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
            } catch (TimeoutException te) {
              // Ignored
            } finally {
              if (jobMetrics.isPresent()) {
                try {
                  jobMetrics.get().stopMetricsReporting();
                } catch (Throwable throwable) {
                  LOG.error("Failed to stop job metrics reporting.", throwable);
                } finally {
                  GobblinMetrics.remove(jobMetrics.get().getName());
                }
              }
            }
          }
        };
        if (!this.isSpeculativeEnabled || gobblinMultiTaskAttempt == null) {
          cleanUpCommitStep.execute();
        } else {
          LOG.info("Adding additional commit step");
          gobblinMultiTaskAttempt.addCleanupCommitStep(cleanUpCommitStep);
        }
      }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      WorkUnit workUnit = (value.toString().endsWith(MULTI_WORK_UNIT_FILE_EXTENSION) ? MultiWorkUnit.createEmpty()
          : WorkUnit.createEmpty());
      SerializationUtils.deserializeState(this.fs, new Path(value.toString()), workUnit);

      if (workUnit instanceof MultiWorkUnit) {
        List<WorkUnit> flattenedWorkUnits =
            JobLauncherUtils.flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits());
        this.workUnits.addAll(flattenedWorkUnits);
      } else {
        this.workUnits.add(workUnit);
      }
    }
  }
}
