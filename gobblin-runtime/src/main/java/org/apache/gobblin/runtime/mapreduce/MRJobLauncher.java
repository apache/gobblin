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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.fsm.FiniteStateMachine;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MultiReporterException;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.JobStateEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.metrics.reporter.util.MetricReportUtils;
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
import org.apache.gobblin.runtime.job.GobblinJobFiniteStateMachine;
import org.apache.gobblin.runtime.job.GobblinJobFiniteStateMachine.JobFSMState;
import org.apache.gobblin.runtime.job.GobblinJobFiniteStateMachine.StateType;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooterFactory;
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
import org.apache.gobblin.util.reflection.RestrictedFieldAccessingUtils;
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
@Slf4j
public class MRJobLauncher extends AbstractJobLauncher {

  private static final String INTERRUPT_JOB_FILE_NAME = "_INTERRUPT_JOB";
  private static final String GOBBLIN_JOB_INTERRUPT_PATH_KEY = "gobblin.jobInterruptPath";

  private static final Logger LOG = LoggerFactory.getLogger(MRJobLauncher.class);

  private static final String JOB_NAME_PREFIX = "Gobblin-";

  private static final String JARS_DIR_NAME = "_jars";
  private static final String FILES_DIR_NAME = "_files";
  static final String INPUT_DIR_NAME = "input";
  private static final String OUTPUT_DIR_NAME = "output";
  private static final String SERIALIZE_PREVIOUS_WORKUNIT_STATES_KEY = "MRJobLauncher.serializePreviousWorkunitStates";
  private static final boolean DEFAULT_SERIALIZE_PREVIOUS_WORKUNIT_STATES = true;

  /**
   * In MR-mode, it is necessary to enable customized progress if speculative execution is required.
   */
  private static final String ENABLED_CUSTOMIZED_PROGRESS = "MRJobLauncher.enabledCustomizedProgress";

  // Configuration that make uploading of jar files more reliable,
  // since multiple Gobblin Jobs are sharing the same jar directory.
  private static final int MAXIMUM_JAR_COPY_RETRY_TIMES_DEFAULT = 5;
  private static final int WAITING_TIME_ON_IMCOMPLETE_UPLOAD = 3000;

  public static final String MR_TYPE_KEY = ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX + "mr.type";
  public static final String MAPPER_TASK_NUM_KEY = ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX + "reporting.mapper.task.num";
  public static final String MAPPER_TASK_ATTEMPT_NUM_KEY = ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX + "reporting.mapper.task.attempt.num";
  public static final String REDUCER_TASK_NUM_KEY = ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX + "reporting.reducer.task.num";
  public static final String REDUCER_TASK_ATTEMPT_NUM_KEY = ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX + "reporting.reducer.task.attempt.num";

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
  private final boolean shouldPersistWorkUnitsThenCancel;

  private final int parallelRunnerThreads;

  private final TaskStateCollectorService taskStateCollectorService;

  private volatile boolean hadoopJobSubmitted = false;

  private final StateStore<TaskState> taskStateStore;

  private final int jarFileMaximumRetry;
  private final Path interruptPath;
  private final GobblinJobFiniteStateMachine fsm;

  public MRJobLauncher(Properties jobProps) throws Exception {
    this(jobProps, null);
  }

  public MRJobLauncher(Properties jobProps, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) throws Exception {
    this(jobProps, new Configuration(), instanceBroker);
  }

  public MRJobLauncher(Properties jobProps, Configuration conf, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) throws Exception {
    this(jobProps, conf, instanceBroker, ImmutableList.of());
  }

  public MRJobLauncher(Properties jobProps, SharedResourcesBroker<GobblinScopeTypes> instanceBroker, List<? extends Tag<?>> metadataTags) throws Exception {
    this(jobProps, new Configuration(), instanceBroker, metadataTags);
  }


  public MRJobLauncher(Properties jobProps, Configuration conf, SharedResourcesBroker<GobblinScopeTypes> instanceBroker,List<? extends Tag<?>> metadataTags)
      throws Exception {
    super(jobProps, metadataTags);

    this.fsm = GobblinJobFiniteStateMachine.builder().jobState(jobContext.getJobState())
        .interruptGracefully(this::interruptGracefully).killJob(this::killJob).build();

    this.conf = conf;
    // Put job configuration properties into the Hadoop configuration so they are available in the mappers
    JobConfigurationUtils.putPropertiesIntoConfiguration(this.jobProps, this.conf);

    // Let the job and all mappers finish even if some mappers fail
    this.conf.set("mapreduce.map.failures.maxpercent", isMapperFailureFatalEnabled(this.jobProps) ? "0" : "100"); // For Hadoop 2.x

    // Do not cancel delegation tokens after job has completed (HADOOP-7002)
    this.conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

    this.fs = buildFileSystem(jobProps, this.conf);

    this.mrJobDir = new Path(
        new Path(this.jobProps.getProperty(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY), this.jobContext.getJobName()),
        this.jobContext.getJobId());
    this.interruptPath = new Path(this.mrJobDir, INTERRUPT_JOB_FILE_NAME);
    if (this.fs.exists(this.mrJobDir)) {
      LOG.warn("Job working directory already exists for job " + this.jobContext.getJobName());
      this.fs.delete(this.mrJobDir, true);
    }
    this.unsharedJarsDir = new Path(this.mrJobDir, JARS_DIR_NAME);

    if (this.jobProps.containsKey(ConfigurationKeys.MR_JARS_BASE_DIR)) {
      Path jarsBaseDir = new Path(this.jobProps.getProperty(ConfigurationKeys.MR_JARS_BASE_DIR));
      String monthSuffix = new SimpleDateFormat("yyyy-MM").format(System.currentTimeMillis());
      cleanUpOldJarsDirIfRequired(this.fs, jarsBaseDir);
      this.jarsDir = new Path(jarsBaseDir, monthSuffix);
    } else if (this.jobProps.containsKey(ConfigurationKeys.MR_JARS_DIR)) {
      this.jarsDir = new Path(this.jobProps.getProperty(ConfigurationKeys.MR_JARS_DIR));
    } else {
      this.jarsDir = this.unsharedJarsDir;
    }

    this.fs.mkdirs(this.mrJobDir);

    this.jobInputPath = new Path(this.mrJobDir, INPUT_DIR_NAME);
    this.jobOutputPath = new Path(this.mrJobDir, OUTPUT_DIR_NAME);
    Path outputTaskStateDir = new Path(this.jobOutputPath, this.jobContext.getJobId());
    this.shouldPersistWorkUnitsThenCancel = isPersistWorkUnitsThenCancelEnabled(this.jobProps);

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
        new TaskStateCollectorService(jobProps, this.jobContext.getJobState(), this.eventBus, this.eventSubmitter,
            taskStateStore, outputTaskStateDir, getIssueRepository());

    this.jarFileMaximumRetry =
        jobProps.containsKey(ConfigurationKeys.MAXIMUM_JAR_COPY_RETRY_TIMES_KEY) ? Integer.parseInt(
            jobProps.getProperty(ConfigurationKeys.MAXIMUM_JAR_COPY_RETRY_TIMES_KEY))
            : MAXIMUM_JAR_COPY_RETRY_TIMES_DEFAULT;

    // One of the most common user mistakes is mis-configuring the FileSystem scheme (e.g. file versus hdfs)
    log.info("Configured fs:{}", fs);
    log.debug("Configuration: {}", conf);
    startCancellationExecutor();
  }

  static void cleanUpOldJarsDirIfRequired(FileSystem fs, Path jarsBaseDir) throws IOException {
    List<FileStatus> jarDirs = Arrays.stream(fs.exists(jarsBaseDir)
        ? fs.listStatus(jarsBaseDir) : new FileStatus[0]).sorted().collect(Collectors.toList());
    if (jarDirs.size() > 2) {
      fs.delete(jarDirs.get(0).getPath(), true);
    }
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
      CountEventBuilder countEventBuilder = new CountEventBuilder(JobEvent.WORK_UNITS_CREATED, workUnits.size());
      this.eventSubmitter.submit(countEventBuilder);
      LOG.info("Emitting WorkUnitsCreated Count: " + countEventBuilder.getCount());

      prepareHadoopJob(workUnits);
      if (this.shouldPersistWorkUnitsThenCancel) {
        // NOTE: `warn` level is hack for including path among automatic troubleshooter 'issues'
        LOG.warn("Cancelling job after persisting workunits beneath: " + this.jobInputPath);
        jobState.setState(JobState.RunningState.CANCELLED);
        return;
      }

      // Start the output TaskState collector service
      this.taskStateCollectorService.startAsync().awaitRunning();

      LOG.info("Launching Hadoop MR job " + this.job.getJobName());
      try (FiniteStateMachine<JobFSMState>.Transition t = this.fsm.startTransition(this.fsm.getEndStateForType(StateType.RUNNING))) {
        try {
          this.job.submit();
        } catch (Throwable exc) {
          t.changeEndState(this.fsm.getEndStateForType(StateType.FAILED));
          throw exc;
        }
        this.hadoopJobSubmitted = true;

        // Set job tracking URL to the Hadoop job tracking URL if it is not set yet
        if (!jobState.contains(ConfigurationKeys.JOB_TRACKING_URL_KEY)) {
          jobState.setProp(ConfigurationKeys.JOB_TRACKING_URL_KEY, this.job.getTrackingURL());
        }
        /**
         * Catch {@link UnallowedTransitionException} only, leaving other failure while submitting MR jobs to catch
         * block afterwards.
         */
      } catch (FiniteStateMachine.UnallowedTransitionException unallowed) {
        LOG.error("Cannot start MR job.", unallowed);
      }

      if (this.fsm.getCurrentState().getStateType().equals(StateType.RUNNING)) {
        TimingEvent mrJobRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.MR_JOB_RUN);
        LOG.info(String.format("Waiting for Hadoop MR job %s to complete", this.job.getJobID()));

        this.job.waitForCompletion(true);
        this.fsm.transitionIfAllowed(fsm.getEndStateForType(StateType.SUCCESS));

        mrJobRunTimer.stop(ImmutableMap.of("hadoopMRJobId", this.job.getJobID().toString()));
      }

      if (this.fsm.getCurrentState().getStateType().equals(StateType.CANCELLED)) {
        return;
      }

      // Create a metrics set for this job run from the Hadoop counters.
      // The metrics set is to be persisted to the metrics store later.
      countersToMetrics(JobMetrics.get(jobName, this.jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY)));
    } catch (Throwable t) {
      throw new RuntimeException("The MR job cannot be submitted due to:", t);
    } finally {
      JobStateEventBuilder eventBuilder = new JobStateEventBuilder(JobStateEventBuilder.MRJobState.MR_JOB_STATE);

      if (!hadoopJobSubmitted) {
        eventBuilder.jobTrackingURL = "";
        eventBuilder.status = JobStateEventBuilder.Status.FAILED;
      } else {
        eventBuilder.jobTrackingURL = this.job.getTrackingURL();
        eventBuilder.status = JobStateEventBuilder.Status.SUCCEEDED;
        if (this.job.getJobState() != JobStatus.State.SUCCEEDED) {
          eventBuilder.status = JobStateEventBuilder.Status.FAILED;
        }
      }
      this.eventSubmitter.submit(eventBuilder);

      // The last iteration of output TaskState collecting will run when the collector service gets stopped
      this.taskStateCollectorService.stopAsync().awaitTerminated();
      cleanUpWorkingDirectory();
    }
  }

  @Override
  protected void executeCancellation() {
    try (FiniteStateMachine<JobFSMState>.Transition transition =
        this.fsm.startTransition(this.fsm.getEndStateForType(StateType.CANCELLED))) {
      if (transition.getStartState().getStateType().equals(StateType.RUNNING)) {
        try {
          killJob();
        } catch (IOException ioe) {
          LOG.error("Failed to kill the Hadoop MR job for job " + this.jobContext.getJobId());
          transition.changeEndState(this.fsm.getEndStateForType(StateType.FAILED));
        }
      }
    } catch (GobblinJobFiniteStateMachine.FailedTransitionCallbackException exc) {
      exc.getTransition().switchEndStateToErrorState();
      exc.getTransition().closeWithoutCallbacks();
    } catch (FiniteStateMachine.UnallowedTransitionException | InterruptedException exc) {
      LOG.error("Failed to cancel job " + this.jobContext.getJobId(), exc);
    }
  }

  /**
   * Attempt a gracious interruption of the running job
   */
  private void interruptGracefully() throws IOException {
    LOG.info("Attempting graceful interruption of job " + this.jobContext.getJobId());

    this.fs.createNewFile(this.interruptPath);

    long waitTimeStart = System.currentTimeMillis();
    while (!this.job.isComplete() && System.currentTimeMillis() < waitTimeStart + 30 * 1000) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        break;
      }
    }

    if (!this.job.isComplete()) {
      LOG.info("Interrupted job did not shut itself down after timeout. Killing job.");
      this.job.killJob();
    }
  }

  private void killJob() throws IOException {
    LOG.info("Killing the Hadoop MR job for job " + this.jobContext.getJobId());
    this.job.killJob();
    // Collect final task states.
    this.taskStateCollectorService.stopAsync().awaitTerminated();
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

    this.job.getConfiguration().set(GOBBLIN_JOB_INTERRUPT_PATH_KEY, this.interruptPath.toString());

    mrJobSetupTimer.stop();
  }

  static boolean isBooleanPropEnabled(Properties props, String propKey, Optional<Boolean> optDefault) {
    return (props.containsKey(propKey) && Boolean.parseBoolean(props.getProperty(propKey)))
        || (optDefault.isPresent() && optDefault.get());
  }

  static boolean isSpeculativeExecutionEnabled(Properties props) {
    return isBooleanPropEnabled(props, JobContext.MAP_SPECULATIVE,
        Optional.of(ConfigurationKeys.DEFAULT_ENABLE_MR_SPECULATIVE_EXECUTION));
  }

  static boolean isCustomizedProgressReportEnabled(Properties properties) {
    return isBooleanPropEnabled(properties, ENABLED_CUSTOMIZED_PROGRESS, Optional.empty());
  }

  static boolean isMapperFailureFatalEnabled(Properties props) {
    return isBooleanPropEnabled(props, ConfigurationKeys.MR_JOB_MAPPER_FAILURE_IS_FATAL_KEY,
        Optional.of(ConfigurationKeys.DEFAULT_MR_JOB_MAPPER_FAILURE_IS_FATAL));
  }

  static boolean isPersistWorkUnitsThenCancelEnabled(Properties props) {
    return isBooleanPropEnabled(props, ConfigurationKeys.MR_PERSIST_WORK_UNITS_THEN_CANCEL_KEY,
        Optional.of(ConfigurationKeys.DEFAULT_MR_PERSIST_WORK_UNITS_THEN_CANCEL));
  }

  @VisibleForTesting
  static void serializeJobState(FileSystem fs, Path mrJobDir, Configuration conf, JobState jobState, Job job)
      throws IOException {
    Path jobStateFilePath = new Path(mrJobDir, JOB_STATE_FILE_NAME);
    // Write the job state with an empty task set (work units are read by the mapper from a different file)
    try (DataOutputStream dataOutputStream = new DataOutputStream(fs.create(jobStateFilePath))) {
      jobState.write(dataOutputStream, false,
          conf.getBoolean(SERIALIZE_PREVIOUS_WORKUNIT_STATES_KEY, DEFAULT_SERIALIZE_PREVIOUS_WORKUNIT_STATES));
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
              + JobLauncherUtils.MULTI_WORK_UNIT_FILE_EXTENSION;
        } else {
          workUnitFileName = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY) + JobLauncherUtils.WORK_UNIT_FILE_EXTENSION;
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
        if (this.shouldPersistWorkUnitsThenCancel) {
          LOG.info("Preserving persisted workunits beneath: " + this.jobInputPath);
        } else {
          this.fs.delete(this.mrJobDir, true);
          LOG.info("Deleted working directory " + this.mrJobDir);
        }
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
    Optional<Counters> counters = Optional.ofNullable(this.job.getCounters());

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
    private Optional<JobMetrics> jobMetrics = Optional.empty();
    private boolean isSpeculativeEnabled;
    private boolean customizedProgressEnabled;
    private final JobState jobState = new JobState();
    private CustomizedProgresser customizedProgresser;

    private static final String CUSTOMIZED_PROGRESSER_FACTORY_CLASS = "customizedProgresser.factoryClass";
    private static final String DEFAULT_CUSTOMIZED_PROGRESSER_FACTORY_CLASS =
        "org.apache.gobblin.runtime.mapreduce.CustomizedProgresserBase$BaseFactory";

    // A list of WorkUnits (flattened for MultiWorkUnits) to be run by this mapper
    private final List<WorkUnit> workUnits = Lists.newArrayList();

    private AutomaticTroubleshooter troubleshooter;

    @Override
    protected void setup(Context context) {
      final State gobblinJobState = HadoopUtils.getStateFromConf(context.getConfiguration());
      TaskAttemptID taskAttemptID = context.getTaskAttemptID();

      troubleshooter =
          AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(gobblinJobState.getProperties()));
      troubleshooter.start();

      try (Closer closer = Closer.create()) {
        // Default for customizedProgressEnabled is false.
        this.customizedProgressEnabled = isCustomizedProgressReportEnabled(gobblinJobState.getProperties());
        this.isSpeculativeEnabled = isSpeculativeExecutionEnabled(gobblinJobState.getProperties());

        String factoryClassName = gobblinJobState.getProperties().getProperty(
            CUSTOMIZED_PROGRESSER_FACTORY_CLASS, DEFAULT_CUSTOMIZED_PROGRESSER_FACTORY_CLASS);
        this.customizedProgresser = Class.forName(factoryClassName).asSubclass(CustomizedProgresser.Factory.class)
            .newInstance().createCustomizedProgresser(context);

        this.fs = FileSystem.get(context.getConfiguration());
        this.taskStateStore =
            new FsStateStore<>(this.fs, FileOutputFormat.getOutputPath(context).toUri().getPath(), TaskState.class);
        String jobStateFileName = context.getConfiguration().get(ConfigurationKeys.JOB_STATE_DISTRIBUTED_CACHE_NAME);
        Optional<URI> jobStateFileUri = getStateFileUriForJob(context.getConfiguration(), jobStateFileName);
        if (jobStateFileUri.isPresent()) {
          SerializationUtils.deserializeStateFromInputStream(
                closer.register(new FileInputStream(jobStateFileUri.get().getPath())), this.jobState);
        } else {
          throw new IOException("Job state file not found: '" + jobStateFileName + "'.");
        }
      } catch (IOException | ReflectiveOperationException e) {
        throw new RuntimeException("Failed to setup the mapper task", e);
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
        gobblinJobState.setProp(entry.getKey(), entry.getValue().unwrapped().toString());
      }

      // add some more MR task related configs

      String[] tokens = taskAttemptID.toString().split("_");
      TaskType taskType = taskAttemptID.getTaskType();
      gobblinJobState.setProp(MR_TYPE_KEY, taskType.name());

      // a task attempt id should be like 'attempt_1592863931636_2371636_m_000003_4'
      if (tokens.length == 6) {
        if (taskType.equals(TaskType.MAP)) {
          gobblinJobState.setProp(MAPPER_TASK_NUM_KEY, tokens[tokens.length - 2]);
          gobblinJobState.setProp(MAPPER_TASK_ATTEMPT_NUM_KEY, tokens[tokens.length - 1]);
        } else if (taskType.equals(TaskType.REDUCE)) {
          gobblinJobState.setProp(REDUCER_TASK_NUM_KEY, tokens[tokens.length - 2]);
          gobblinJobState.setProp(REDUCER_TASK_ATTEMPT_NUM_KEY, tokens[tokens.length - 1]);
        }
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
      if (Boolean.parseBoolean(configuration.get(ConfigurationKeys.METRICS_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_ENABLED))) {
        this.jobMetrics = Optional.of(JobMetrics.get(this.jobState));
        try {
          this.jobMetrics.get().startMetricReportingWithFileSuffix(gobblinJobState, taskAttemptID.toString());
        } catch (MultiReporterException ex) {
          //Fail the task if metric/event reporting failure is configured to be fatal.
          boolean isMetricReportingFailureFatal = configuration.getBoolean(ConfigurationKeys.GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL,
              ConfigurationKeys.DEFAULT_GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL);
          boolean isEventReportingFailureFatal = configuration.getBoolean(ConfigurationKeys.GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL,
                  ConfigurationKeys.DEFAULT_GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL);
          if (MetricReportUtils.shouldThrowException(LOG, ex, isMetricReportingFailureFatal, isEventReportingFailureFatal)) {
            throw new RuntimeException(ex);
          }
        }
      }

      AbstractJobLauncher.setDefaultAuthenticator(this.jobState.getProperties());
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      this.setup(context);

      Path interruptPath = new Path(context.getConfiguration().get(GOBBLIN_JOB_INTERRUPT_PATH_KEY));
      if (this.fs.exists(interruptPath)) {
        LOG.info(String.format("Found interrupt path %s indicating the driver has interrupted the job, aborting mapper.", interruptPath));
        return;
      }

      GobblinMultiTaskAttempt gobblinMultiTaskAttempt = null;
      try {
        // De-serialize and collect the list of WorkUnits to run
        while (context.nextKeyValue()) {
          this.map(context.getCurrentKey(), context.getCurrentValue(), context);
        }

        // org.apache.hadoop.util.Progress.complete will set the progress to 1.0f eventually so we don't have to
        // set it in finally block.
        if (customizedProgressEnabled) {
          setProgressInMapper(customizedProgresser.getCustomizedProgress(), context);
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
                multiTaskAttemptCommitPolicy, jobBroker, troubleshooter.getIssueRepository(), (gmta) -> {
                  try {
                    return this.fs.exists(interruptPath);
                  } catch (IOException ioe) {
                    return false;
                  }
                });

        if (this.isSpeculativeEnabled) {
          LOG.info("will not commit in task attempt");
          GobblinOutputCommitter gobblinOutputCommitter = (GobblinOutputCommitter) context.getOutputCommitter();
          gobblinOutputCommitter.getAttemptIdToMultiTaskAttempt()
              .put(context.getTaskAttemptID().toString(), gobblinMultiTaskAttempt);
        }
      } finally {
        try {
          troubleshooter.refineIssues();
          troubleshooter.logIssueSummary();
          troubleshooter.stop();
        } catch (Exception e) {
          LOG.error("Failed to report issues from automatic troubleshooter", e);
        }

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
      this.workUnits.addAll(JobLauncherUtils.loadFlattenedWorkUnits(this.fs, new Path(value.toString())));
    }

    /** @return {@link URI} if a distributed cache file matches `jobStateFileName` */
    protected Optional<URI> getStateFileUriForJob(Configuration conf, String jobStateFileName) throws IOException {
      for (Path dcPath : DistributedCache.getLocalCacheFiles(conf)) {
        if (dcPath.getName().equals(jobStateFileName)) {
          return Optional.of(dcPath.toUri());
        }
      }
      return Optional.empty();
    }

    /**
     * Setting progress within implementation of {@link Mapper} for reporting progress.
     * Gobblin (when running in MR mode) used to report progress only in {@link GobblinWorkUnitsInputFormat} while
     * deserializing {@link WorkUnit} in MapReduce job. In that scenario, whenever workunit is deserialized (but not yet
     * executed) the progress will be reported as 1.0f. This could implicitly disable the feature of speculative-execution
     * provided by MR-framework as the latter is looking at the progress to determine if speculative-execution is necessary
     * to trigger or not.
     *
     * Different application of Gobblin should have customized logic on calculating progress.
     */
    void setProgressInMapper(float progress, Context context) {
      try {
        WrappedMapper.Context wrappedContext = ((WrappedMapper.Context) context);
        Object contextImpl = RestrictedFieldAccessingUtils.getRestrictedFieldByReflection(wrappedContext, "mapContext", wrappedContext.getClass());
        ((org.apache.hadoop.mapred.Task.TaskReporter)RestrictedFieldAccessingUtils
            .getRestrictedFieldByReflectionRecursively(contextImpl, "reporter", MapContextImpl.class)).setProgress(progress);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
