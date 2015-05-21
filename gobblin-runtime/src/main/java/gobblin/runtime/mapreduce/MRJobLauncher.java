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

package gobblin.runtime.mapreduce;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.Instrumented;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.FileBasedJobLock;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLock;
import gobblin.runtime.JobState;
import gobblin.runtime.Task;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskState;
import gobblin.runtime.TaskStateTracker;
import gobblin.runtime.util.JobMetrics;
import gobblin.runtime.util.MetricGroup;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobLauncherUtils;


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
 * @author ynli
 */
public class MRJobLauncher extends AbstractJobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(MRJobLauncher.class);

  private static final String JOB_NAME_PREFIX = "Gobblin-";

  private static final String WORK_UNIT_FILE_EXTENSION = ".wu";
  private static final String MULTI_WORK_UNIT_FILE_EXTENSION = ".mwu";

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final Configuration conf;
  private final FileSystem fs;
  private final Job job;
  private final Path mrJobDir;

  public MRJobLauncher(Properties sysProps, Properties jobProps)
      throws Exception {
    this(sysProps, jobProps, new Configuration());
  }

  public MRJobLauncher(Properties properties, Properties jobProps, Configuration conf)
      throws Exception {
    super(properties, jobProps);

    this.conf = conf;

    // Add job config properties that also contains all framework config properties
    for (String name : this.jobProps.stringPropertyNames()) {
      this.conf.set(name, this.jobProps.getProperty(name));
    }

    // Let the job and all mappers finish even if some mappers fail
    this.conf.set("mapred.max.map.failures.percent", "100"); // For Hadoop 1.x
    this.conf.set("mapreduce.map.failures.maxpercent", "100"); // For Hadoop 2.x

    // Do not cancel delegation tokens after job has completed (HADOOP-7002)
    this.conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

    URI fsUri = URI.create(this.sysProps.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.fs = FileSystem.get(fsUri, conf);

    this.mrJobDir = new Path(this.jobProps.getProperty(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY), jobContext.getJobName());
    if (this.fs.exists(this.mrJobDir)) {
      LOG.warn("Job working directory already exists for job " + jobContext.getJobName());
      this.fs.delete(this.mrJobDir, true);
    }
    this.fs.mkdirs(this.mrJobDir);

    // Add dependent jars/files
    addDependencies();

    // Finally create the Hadoop job after all updates to conf are already made (including
    // adding dependent jars/files to the DistributedCache that also updates the conf)
    this.job = Job.getInstance(this.conf, JOB_NAME_PREFIX + this.jobContext.getJobName());

    startCancellationExecutor();
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (!this.job.isComplete()) {
        LOG.info("Killing the Hadoop MR job for job " + this.jobContext.getJobId());
        this.job.killJob();
      }
    } finally {
      super.close();
    }
  }

  @Override
  protected void runWorkUnits(List<WorkUnit> workUnits)
      throws Exception {
    String jobName = this.jobContext.getJobName();
    JobState jobState = this.jobContext.getJobState();

    try {
      // Delete any staging directories that already exist before the Hadoop MR job starts
      JobLauncherUtils.cleanStagingData(JobLauncherUtils.flattenWorkUnits(workUnits), LOG);

      Path jobOutputPath = prepareHadoopJob(workUnits);
      LOG.info("Launching Hadoop MR job " + this.job.getJobName());
      this.job.submit();

      // Set job tracking URL to the Hadoop job tracking URL if it is not set yet
      if (!jobState.contains(ConfigurationKeys.JOB_TRACKING_URL_KEY)) {
        jobState.setProp(ConfigurationKeys.JOB_TRACKING_URL_KEY, this.job.getTrackingURL());
      }

      LOG.info(String.format("Waiting for Hadoop MR job %s to complete", this.job.getJobID()));
      this.job.waitForCompletion(true);

      if (this.cancellationRequested) {
        // Wait for the cancellation execution if it has been requested
        synchronized (this.cancellationExecution) {
          if (this.cancellationExecuted) {
            return;
          }
        }
      }

      jobState.setState(this.job.isSuccessful() ? JobState.RunningState.SUCCESSFUL : JobState.RunningState.FAILED);
      // Collect the output task states and add them to the job state
      jobState.addTaskStates(
          collectOutput(new Path(jobOutputPath, this.jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY))));

      // Create a metrics set for this job run from the Hadoop counters.
      // The metrics set is to be persisted to the metrics store later.
      countersToMetrics(this.job.getCounters(),
          JobMetrics.get(jobName, this.jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY)));
    } finally {
      cleanUpWorkingDirectory();
    }
  }

  @Override
  protected JobLock getJobLock()
      throws IOException {
    return new FileBasedJobLock(this.fs, this.jobProps.getProperty(ConfigurationKeys.JOB_LOCK_DIR_KEY),
        this.jobContext.getJobName());
  }

  @Override
  protected void executeCancellation() {
    try {
      if (!this.job.isComplete()) {
        LOG.info("Killing the Hadoop MR job for job " + this.jobContext.getJobId());
        this.job.killJob();
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
  private void addDependencies() throws IOException {
    Path jarFileDir = new Path(this.mrJobDir, "_jars");

    // Add framework jars to the classpath for the mappers/reducer
    if (jobProps.containsKey(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY)) {
      addJars(jarFileDir, jobProps.getProperty(ConfigurationKeys.FRAMEWORK_JAR_FILES_KEY));
    }

    // Add job-specific jars to the classpath for the mappers
    if (jobProps.containsKey(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      addJars(jarFileDir, jobProps.getProperty(ConfigurationKeys.JOB_JAR_FILES_KEY));
    }

    // Add other files (if any) the job depends on to DistributedCache
    if (jobProps.containsKey(ConfigurationKeys.JOB_LOCAL_FILES_KEY)) {
      addLocalFiles(new Path(this.mrJobDir, "_files"), jobProps.getProperty(ConfigurationKeys.JOB_LOCAL_FILES_KEY));
    }

    // Add files (if any) already on HDFS that the job depends on to DistributedCache
    if (jobProps.containsKey(ConfigurationKeys.JOB_HDFS_FILES_KEY)) {
      addHDFSFiles(jobProps.getProperty(ConfigurationKeys.JOB_HDFS_FILES_KEY));
    }
  }

  /**
   * Prepare the Hadoop MR job, including configuring the job and setting up the input/output paths.
   */
  private Path prepareHadoopJob(List<WorkUnit> workUnits)
      throws IOException {
    this.job.setJarByClass(MRJobLauncher.class);
    this.job.setMapperClass(TaskRunner.class);

    // The job is mapper-only
    this.job.setNumReduceTasks(0);

    this.job.setInputFormatClass(NLineInputFormat.class);
    this.job.setOutputFormatClass(GobblinOutputFormat.class);
    this.job.setMapOutputKeyClass(NullWritable.class);
    this.job.setMapOutputValueClass(NullWritable.class);

    // Turn off speculative execution
    this.job.setSpeculativeExecution(false);

    // Job input path is where input work unit files are stored
    Path jobInputPath = new Path(this.mrJobDir, "input");

    // Prepare job input
    Path jobInputFile = prepareJobInput(jobInputPath, workUnits);
    NLineInputFormat.addInputPath(this.job, jobInputFile);

    // Job output path is where serialized task states are stored
    Path jobOutputPath = new Path(this.mrJobDir, "output");
    SequenceFileOutputFormat.setOutputPath(this.job, jobOutputPath);

    if (this.jobProps.containsKey(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY)) {
      // When there is a limit on the number of mappers, each mapper may run
      // multiple tasks if the total number of tasks is larger than the limit.
      int maxMappers = Integer.parseInt(this.jobProps.getProperty(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY));
      if (workUnits.size() > maxMappers) {
        int numTasksPerMapper =
            workUnits.size() % maxMappers == 0 ? workUnits.size() / maxMappers : workUnits.size() / maxMappers + 1;
        NLineInputFormat.setNumLinesPerSplit(this.job, numTasksPerMapper);
      }
    }

    return jobOutputPath;
  }

  /**
   * Add framework or job-specific jars to the classpath through DistributedCache
   * so the mappers can use them.
   */
  private void addJars(Path jarFileDir, String jarFileList)
      throws IOException {
    LocalFileSystem lfs = FileSystem.getLocal(this.conf);
    for (String jarFile : SPLITTER.split(jarFileList)) {
      Path srcJarFile = new Path(jarFile);
      FileStatus[] fileStatusList = lfs.globStatus(srcJarFile);
      for (FileStatus status : fileStatusList) {
        // DistributedCache requires absolute path, so we need to use makeQualified.
        Path destJarFile = new Path(this.fs.makeQualified(jarFileDir), status.getPath().getName());
        // Copy the jar file from local file system to HDFS
        this.fs.copyFromLocalFile(status.getPath(), destJarFile);
        // Then add the jar file on HDFS to the classpath
        LOG.info(String.format("Adding %s to classpath", destJarFile));
        DistributedCache.addFileToClassPath(destJarFile, this.conf, this.fs);
      }
    }
  }

  /**
   * Add local non-jar files the job depends on to DistributedCache.
   */
  private void addLocalFiles(Path jobFileDir, String jobFileList)
      throws IOException {
    DistributedCache.createSymlink(this.conf);
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
      DistributedCache.addCacheFile(destFileUri, this.conf);
    }
  }

  /**
   * Add non-jar files already on HDFS that the job depends on to DistributedCache.
   */
  private void addHDFSFiles(String jobFileList)
      throws IOException {
    DistributedCache.createSymlink(this.conf);
    for (String jobFile : SPLITTER.split(jobFileList)) {
      Path srcJobFile = new Path(jobFile);
      // Create a URI that is in the form path#symlink
      URI srcFileUri = URI.create(srcJobFile.toUri().getPath() + "#" + srcJobFile.getName());
      LOG.info(String.format("Adding %s to DistributedCache", srcFileUri));
      // Finally add the file to DistributedCache with a symlink named after the file name
      DistributedCache.addCacheFile(srcFileUri, this.conf);
    }
  }

  /**
   * Prepare the job input.
   * @throws IOException
   */
  private Path prepareJobInput(Path jobInputPath, List<WorkUnit> workUnits) throws IOException {
    Closer closer = Closer.create();
    try {
      // The job input is a file named after the job ID listing all work unit file paths
      Path jobInputFile = new Path(jobInputPath, this.jobContext.getJobId() + ".wulist");
      // Open the job input file
      OutputStream os = closer.register(this.fs.create(jobInputFile));
      Writer osw = closer.register(new OutputStreamWriter(os, ConfigurationKeys.DEFAULT_CHARSET_ENCODING));
      Writer bw = closer.register(new BufferedWriter(osw));

      // Serialize each work unit into a file named after the task ID
      for (WorkUnit workUnit : workUnits) {
        Closer workUnitFileCloser = Closer.create();
        try {
          Path workUnitFile =
              new Path(jobInputPath, workUnit.getProp(ConfigurationKeys.TASK_ID_KEY)
                  + ((workUnit instanceof MultiWorkUnit) ? MULTI_WORK_UNIT_FILE_EXTENSION : WORK_UNIT_FILE_EXTENSION));
          os = workUnitFileCloser.register(this.fs.create(workUnitFile));
          DataOutputStream dos = workUnitFileCloser.register(new DataOutputStream(os));
          workUnit.write(dos);
          // Append the work unit file path to the job input file
          bw.write(workUnitFile.toUri().getPath() + "\n");
        } catch (Throwable t) {
          throw workUnitFileCloser.rethrow(t);
        } finally {
          workUnitFileCloser.close();
        }
      }

      return jobInputFile;
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /**
   * Collect the output {@link TaskState}s of the job as a list.
   */
  private List<TaskState> collectOutput(Path taskStatePath)
      throws IOException {
    List<TaskState> taskStates = Lists.newArrayList();

    FileStatus[] fileStatuses = this.fs.listStatus(taskStatePath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(TASK_STATE_STORE_TABLE_SUFFIX);
      }
    });
    if (fileStatuses == null || fileStatuses.length == 0) {
      return taskStates;
    }

    for (FileStatus status : fileStatuses) {
      Closer closer = Closer.create();
      try {
        // Read out the task states
        SequenceFile.Reader reader =
            closer.register(new SequenceFile.Reader(this.fs, status.getPath(), this.fs.getConf()));
        Text text = new Text();
        TaskState taskState = new TaskState();
        while (reader.next(text, taskState)) {
          taskStates.add(taskState);
          taskState = new TaskState();
        }
      } catch (Throwable t) {
        throw closer.rethrow(t);
      } finally {
        closer.close();
      }
    }

    LOG.info(String.format("Collected task state of %d completed tasks", taskStates.size()));

    return taskStates;
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
   * Create a {@link gobblin.metrics.GobblinMetrics} instance for this job run from the Hadoop counters.
   */
  private void countersToMetrics(Counters counters, GobblinMetrics metrics) {
    // Write job-level counters
    CounterGroup jobCounterGroup = counters.getGroup(MetricGroup.JOB.name());
    for (Counter jobCounter : jobCounterGroup) {
      metrics.getCounter(jobCounter.getName()).inc(jobCounter.getValue());
    }

    // Write task-level counters
    CounterGroup taskCounterGroup = counters.getGroup(MetricGroup.TASK.name());
    for (Counter taskCounter : taskCounterGroup) {
      metrics.getCounter(taskCounter.getName()).inc(taskCounter.getValue());
    }
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

    // A list of WorkUnits (flattened for MultiWorkUnits) to be run by this mapper
    private final List<WorkUnit> workUnits = Lists.newArrayList();

    @Override
    protected void setup(Context context) {
      try {
        this.fs = FileSystem.get(context.getConfiguration());
        this.taskStateStore = new FsStateStore<TaskState>(
            this.fs, SequenceFileOutputFormat.getOutputPath(context).toUri().getPath(), TaskState.class);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to setup the mapper task", ioe);
      }

      this.taskExecutor = new TaskExecutor(context.getConfiguration());
      this.taskStateTracker = new MRTaskStateTracker(context);
      this.serviceManager = new ServiceManager(Lists.newArrayList(this.taskExecutor, this.taskStateTracker));
      try {
        this.serviceManager.startAsync().awaitHealthy(5, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        LOG.error("Timed out while waiting for the service manager to start up", te);
        throw new RuntimeException(te);
      }
    }

    @Override
    public void run(Context context)
        throws IOException, InterruptedException {
      this.setup(context);

      try {
        // De-serialize and collect the list of WorkUnits to run
        while (context.nextKeyValue()) {
          this.map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        // Actually run the list of WorkUnits
        runWorkUnits(this.workUnits);
      } finally {
        this.cleanup(context);
      }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      WorkUnit workUnit =
          (value.toString().endsWith(MULTI_WORK_UNIT_FILE_EXTENSION) ? new MultiWorkUnit() : new WorkUnit());
      Closer closer = Closer.create();
      // Deserialize the work unit of the assigned task
      try {
        InputStream is = closer.register(this.fs.open(new Path(value.toString())));
        DataInputStream dis = closer.register((new DataInputStream(is)));
        workUnit.readFields(dis);
      } catch (Throwable t) {
        throw closer.rethrow(t);
      } finally {
        closer.close();
      }

      if (workUnit instanceof MultiWorkUnit) {
        this.workUnits.addAll(((MultiWorkUnit) workUnit).getWorkUnits());
      } else {
        this.workUnits.add(workUnit);
      }
    }

    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      try {
        this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        // Ignored
      }
    }

    /**
     * Run the given list of {@link WorkUnit}s sequentially. If any work unit/task fails,
     * an {@link java.io.IOException} is thrown so the mapper is failed and retried.
     */
    private void runWorkUnits(List<WorkUnit> workUnits)
        throws IOException, InterruptedException {
      if (workUnits.isEmpty()) {
        LOG.warn("No work units to run");
        return;
      }

      String jobId = workUnits.get(0).getProp(ConfigurationKeys.JOB_ID_KEY);
      JobMetrics jobMetrics = JobMetrics.get(null, jobId);

      for (WorkUnit workUnit : workUnits) {
        workUnit.setProp(Instrumented.METRIC_CONTEXT_NAME_KEY, jobMetrics.getName());
        String taskId = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY);
        // Delete the task state file for the task if it already exists.
        // This usually happens if the task is retried upon failure.
        if (this.taskStateStore.exists(jobId, taskId + TASK_STATE_STORE_TABLE_SUFFIX)) {
          this.taskStateStore.delete(jobId, taskId + TASK_STATE_STORE_TABLE_SUFFIX);
        }
      }

      CountDownLatch countDownLatch = new CountDownLatch(workUnits.size());
      List<Task> tasks = AbstractJobLauncher.submitWorkUnits(
          jobId, workUnits, this.taskStateTracker, this.taskExecutor, countDownLatch);

      LOG.info(String.format("Waiting for submitted tasks of job %s to complete...", jobId));
      while (countDownLatch.getCount() > 0) {
        LOG.info(String
            .format("%d out of %d tasks of job %s are running", countDownLatch.getCount(), workUnits.size(), jobId));
        countDownLatch.await(10, TimeUnit.SECONDS);
      }
      LOG.info(String.format("All tasks of job %s have completed", jobId));

      boolean hasTaskFailure = false;
      for (Task task : tasks) {
        LOG.info("Writing task state for task " + task.getTaskId());
        this.taskStateStore.put(task.getJobId(), task.getTaskId() + TASK_STATE_STORE_TABLE_SUFFIX, task.getTaskState());

        if (task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.FAILED) {
          hasTaskFailure = true;
        }
      }

      if (hasTaskFailure) {
        throw new IOException("Not all tasks completed successfully");
      }
    }
  }
}
