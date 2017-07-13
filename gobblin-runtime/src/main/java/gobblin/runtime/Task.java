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

package gobblin.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.NoArgsConstructor;

import gobblin.Constructs;
import gobblin.commit.SpeculativeAttemptAwareConstruct;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.fork.CopyHelper;
import gobblin.fork.CopyNotSupportedException;
import gobblin.fork.Copyable;
import gobblin.fork.ForkOperator;
import gobblin.instrumented.extractor.InstrumentedExtractorBase;
import gobblin.instrumented.extractor.InstrumentedExtractorDecorator;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.TaskEvent;
import gobblin.publisher.DataPublisher;
import gobblin.publisher.SingleTaskDataPublisher;
import gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.runtime.fork.AsynchronousFork;
import gobblin.runtime.fork.Fork;
import gobblin.runtime.fork.SynchronousFork;
import gobblin.runtime.task.TaskIFace;
import gobblin.runtime.util.TaskMetrics;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.stream.RecordEnvelope;
import gobblin.source.extractor.StreamingExtractor;
import gobblin.state.ConstructState;
import gobblin.util.ConfigUtils;
import gobblin.writer.AcknowledgableWatermark;
import gobblin.writer.DataWriter;
import gobblin.writer.FineGrainedWatermarkTracker;
import gobblin.writer.MultiWriterWatermarkManager;
import gobblin.writer.TrackerBasedWatermarkManager;
import gobblin.writer.WatermarkAwareWriter;
import gobblin.writer.WatermarkManager;
import gobblin.writer.WatermarkStorage;


/**
 * A physical unit of execution for a Gobblin {@link gobblin.source.workunit.WorkUnit}.
 *
 * <p>
 *     Each task is executed by a single thread in a thread pool managed by the {@link TaskExecutor}
 *     and each {@link Fork} of the task is executed in a separate thread pool also managed by the
 *     {@link TaskExecutor}.
 *
 *     Each {@link Task} consists of the following steps:
 *     <ul>
 *       <li>Extracting, converting, and forking the source schema.</li>
 *       <li>Extracting, converting, doing row-level quality checking, and forking each data record.</li>
 *       <li>Putting each forked record into the record queue managed by each {@link Fork}.</li>
 *       <li>Committing output data of each {@link Fork} once all {@link Fork}s finish.</li>
 *       <li>Cleaning up and exiting.</li>
 *     </ul>
 *
 *     Each {@link Fork} consists of the following steps:
 *     <ul>
 *       <li>Getting the next record off the record queue.</li>
 *       <li>Converting the record and doing row-level quality checking if applicable.</li>
 *       <li>Writing the record out if it passes the quality checking.</li>
 *       <li>Cleaning up and exiting once all the records have been processed.</li>
 *     </ul>
 * </p>
 *
 * @author Yinan Li
 */
@NoArgsConstructor(force = true)
public class Task implements TaskIFace {

  private static final Logger LOG = LoggerFactory.getLogger(Task.class);

  private final String jobId;
  private final String taskId;
  private final String taskKey;
  private final TaskContext taskContext;
  private final TaskState taskState;
  private final TaskStateTracker taskStateTracker;
  private final TaskExecutor taskExecutor;
  private final Optional<CountDownLatch> countDownLatch;
  private final Map<Optional<Fork>, Optional<Future<?>>> forks = Maps.newLinkedHashMap();

  // Number of task retries
  private final AtomicInteger retryCount = new AtomicInteger();

  private final Converter converter;
  private final InstrumentedExtractorBase extractor;
  private final RowLevelPolicyChecker rowChecker;
  private final ExecutionModel taskMode;
  private final String watermarkingStrategy;
  private final Optional<WatermarkManager> watermarkManager;
  private final Optional<FineGrainedWatermarkTracker> watermarkTracker;
  private final Optional<WatermarkStorage> watermarkStorage;

  private final Closer closer;

  private long startTime;
  private volatile long lastRecordPulledTimestampMillis;
  private final AtomicLong recordsPulled;

  private final AtomicBoolean shutdownRequested;
  private volatile long shutdownRequestedTime = Long.MAX_VALUE;
  private final CountDownLatch shutdownLatch;

  /**
   * Instantiate a new {@link Task}.
   *
   * @param context a {@link TaskContext} containing all necessary information to construct and run a {@link Task}
   * @param taskStateTracker a {@link TaskStateTracker} for tracking task state
   * @param taskExecutor a {@link TaskExecutor} for executing the {@link Task} and its {@link Fork}s
   * @param countDownLatch an optional {@link java.util.concurrent.CountDownLatch} used to signal the task completion
   */
  public Task(TaskContext context, TaskStateTracker taskStateTracker, TaskExecutor taskExecutor,
      Optional<CountDownLatch> countDownLatch) {
    this.taskContext = context;
    this.taskState = context.getTaskState();
    this.jobId = this.taskState.getJobId();
    this.taskId = this.taskState.getTaskId();
    this.taskKey = this.taskState.getTaskKey();
    this.taskStateTracker = taskStateTracker;
    this.taskExecutor = taskExecutor;
    this.countDownLatch = countDownLatch;
    this.closer = Closer.create();
    this.closer.register(this.taskState.getTaskBrokerNullable());
    this.extractor =
        closer.register(new InstrumentedExtractorDecorator<>(this.taskState, this.taskContext.getExtractor()));

    this.converter = closer.register(new MultiConverter(this.taskContext.getConverters()));
    try {
      this.rowChecker = closer.register(this.taskContext.getRowLevelPolicyChecker());
    } catch (Exception e) {
      try {
        closer.close();
      } catch (Throwable t) {
        LOG.error("Failed to close all open resources", t);
      }
      throw new RuntimeException("Failed to instantiate row checker.", e);
    }

    this.taskMode = getExecutionModel(this.taskState);
    this.recordsPulled = new AtomicLong(0);
    this.lastRecordPulledTimestampMillis = 0;
    this.shutdownRequested = new AtomicBoolean(false);
    this.shutdownLatch = new CountDownLatch(1);

    // Setup Streaming constructs

    this.watermarkingStrategy = "FineGrain"; // TODO: Configure

    if (isStreamingTask()) {
      Extractor underlyingExtractor = this.taskContext.getRawSourceExtractor();
      if (!(underlyingExtractor instanceof StreamingExtractor)) {
        LOG.error(
            "Extractor {}  is not an instance of StreamingExtractor but the task is configured to run in continuous mode",
            underlyingExtractor.getClass().getName());
        throw new TaskInstantiationException("Extraction " + underlyingExtractor.getClass().getName()
            + " is not an instance of StreamingExtractor but the task is configured to run in continuous mode");
      }

      this.watermarkStorage = Optional.of(taskContext.getWatermarkStorage());
      Config config;
      try {
        config = ConfigUtils.propertiesToConfig(taskState.getProperties());
      } catch (Exception e) {
        LOG.warn("Failed to deserialize taskState into Config.. continuing with an empty config", e);
        config = ConfigFactory.empty();
      }

      long commitIntervalMillis = ConfigUtils.getLong(config,
          TaskConfigurationKeys.STREAMING_WATERMARK_COMMIT_INTERVAL_MILLIS,
          TaskConfigurationKeys.DEFAULT_STREAMING_WATERMARK_COMMIT_INTERVAL_MILLIS);
      if (watermarkingStrategy.equals("FineGrain")) { // TODO: Configure
        this.watermarkTracker = Optional.of(this.closer.register(new FineGrainedWatermarkTracker(config)));
        this.watermarkManager = Optional.of((WatermarkManager) this.closer.register(
            new TrackerBasedWatermarkManager(this.watermarkStorage.get(), this.watermarkTracker.get(),
                commitIntervalMillis, Optional.of(this.LOG))));

      } else {
        // writer-based watermarking
        this.watermarkManager = Optional.of((WatermarkManager) this.closer.register(
            new MultiWriterWatermarkManager(this.watermarkStorage.get(), commitIntervalMillis, Optional.of(this.LOG))));
        this.watermarkTracker = Optional.absent();
      }
    } else {
      this.watermarkManager = Optional.absent();
      this.watermarkTracker = Optional.absent();
      this.watermarkStorage = Optional.absent();
    }
  }


  public static ExecutionModel getExecutionModel(State state) {
    String mode = state
        .getProp(TaskConfigurationKeys.TASK_EXECUTION_MODE, TaskConfigurationKeys.DEFAULT_TASK_EXECUTION_MODE);
    try {
      return ExecutionModel.valueOf(mode.toUpperCase());
    } catch (Exception e) {
      LOG.warn("Could not find an execution model corresponding to {}, returning {}", mode, ExecutionModel.BATCH, e);
      return ExecutionModel.BATCH;
    }
  }

  protected boolean areSingleBranchTasksSynchronous(TaskContext taskContext) {
    return BooleanUtils.toBoolean(taskContext.getTaskState()
            .getProp(TaskConfigurationKeys.TASK_IS_SINGLE_BRANCH_SYNCHRONOUS, TaskConfigurationKeys.DEFAULT_TASK_IS_SINGLE_BRANCH_SYNCHRONOUS));
  }

  protected boolean isStreamingTask() {
    return this.taskMode.equals(ExecutionModel.STREAMING);
  }

  public boolean awaitShutdown(long timeoutInMillis)
      throws InterruptedException {
    return this.shutdownLatch.await(timeoutInMillis, TimeUnit.MILLISECONDS);
  }

  private void completeShutdown() {
    this.shutdownLatch.countDown();
  }

  private boolean shutdownRequested() {
    if (!this.shutdownRequested.get()) {
      this.shutdownRequested.set(Thread.currentThread().isInterrupted());
    }
    return this.shutdownRequested.get();
  }

  public void shutdown() {
    this.shutdownRequested.set(true);
    this.shutdownRequestedTime = Math.min(System.currentTimeMillis(), this.shutdownRequestedTime);
  }

  public String getProgress() {
    long currentTime = System.currentTimeMillis();
    long lastRecordTimeElapsed = currentTime - this.lastRecordPulledTimestampMillis;
    if (isStreamingTask()) {
      WatermarkManager.CommitStatus commitStatus = this.watermarkManager.get().getCommitStatus();
      long lastWatermarkCommitTimeElapsed = currentTime - commitStatus.getLastWatermarkCommitSuccessTimestampMillis();

      String progressString = String.format("recordsPulled:%d, lastRecordExtracted: %d ms ago, "
              + "lastWatermarkCommitted: %d ms ago, lastWatermarkCommitted: %s", this.recordsPulled.get(),
          lastRecordTimeElapsed, lastWatermarkCommitTimeElapsed, commitStatus.getLastCommittedWatermarks());
      return progressString;
    } else {
      String progressString = String
          .format("recordsPulled:%d, lastRecordExtracted: %d ms ago", this.recordsPulled.get(), lastRecordTimeElapsed);
      return progressString;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {
    MDC.put(ConfigurationKeys.TASK_KEY_KEY, this.taskKey);
    this.startTime = System.currentTimeMillis();
    this.taskState.setStartTime(startTime);
    this.taskState.setWorkingState(WorkUnitState.WorkingState.RUNNING);

    // Clear the map so it starts with a fresh set of forks for each run/retry
    this.forks.clear();
    try {

      if (this.taskState.getPropAsBoolean(ConfigurationKeys.TASK_SYNCHRONOUS_EXECUTION_MODEL_KEY,
          ConfigurationKeys.DEFAULT_TASK_SYNCHRONOUS_EXECUTION_MODEL)) {
        LOG.warn("Synchronous task execution model is deprecated. Please consider using stream model.");
        runSynchronousModel();
      } else {
        new StreamModelTaskRunner(this, this.taskState, this.closer, this.taskContext, this.extractor,
            this.converter, this.rowChecker, this.taskExecutor, this.taskMode, this.shutdownRequested,
            this.watermarkTracker, this.watermarkManager, this.watermarkStorage, this.forks, this.watermarkingStrategy).run();
      }

      LOG.info("Extracted " + this.recordsPulled + " data records");
      LOG.info("Row quality checker finished with results: " + this.rowChecker.getResults().getResults());

      this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXTRACTED, this.recordsPulled);
      this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, extractor.getExpectedRecordCount());

      if (!this.forks.keySet().stream().map(Optional::get).allMatch(Fork::isSucceeded)) {
        throw new RuntimeException("Some forks failed.");
      }

      //TODO: Move these to explicit shutdown phase
      if (watermarkManager.isPresent()) {
        watermarkManager.get().close();
      }
      if (watermarkTracker.isPresent()) {
        watermarkTracker.get().close();
      }
    } catch (Throwable t) {
      failTask(t);
    } finally {
      this.taskStateTracker.onTaskRunCompletion(this);
      completeShutdown();
    }
  }

  @Deprecated
  private void runSynchronousModel() throws Exception {
    // Get the fork operator. By default IdentityForkOperator is used with a single branch.
    ForkOperator forkOperator = closer.register(this.taskContext.getForkOperator());
    forkOperator.init(this.taskState);
    int branches = forkOperator.getBranches(this.taskState);
    // Set fork.branches explicitly here so the rest task flow can pick it up
    this.taskState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, branches);

    // Extract, convert, and fork the source schema.
    Object schema = converter.convertSchema(extractor.getSchema(), this.taskState);
    List<Boolean> forkedSchemas = forkOperator.forkSchema(this.taskState, schema);
    if (forkedSchemas.size() != branches) {
      throw new ForkBranchMismatchException(String
          .format("Number of forked schemas [%d] is not equal to number of branches [%d]", forkedSchemas.size(),
              branches));
    }

    if (inMultipleBranches(forkedSchemas) && !(CopyHelper.isCopyable(schema))) {
      throw new CopyNotSupportedException(schema + " is not copyable");
    }

    RowLevelPolicyCheckResults rowResults = new RowLevelPolicyCheckResults();

    if (!areSingleBranchTasksSynchronous(this.taskContext) || branches > 1) {
      // Create one fork for each forked branch
      for (int i = 0; i < branches; i++) {
        if (forkedSchemas.get(i)) {
          AsynchronousFork fork = closer.register(
              new AsynchronousFork(this.taskContext, schema instanceof Copyable ? ((Copyable) schema).copy() : schema,
                  branches, i, this.taskMode));
          configureStreamingFork(fork, watermarkingStrategy);
          // Run the Fork
          this.forks.put(Optional.<Fork>of(fork), Optional.<Future<?>>of(this.taskExecutor.submit(fork)));
        } else {
          this.forks.put(Optional.<Fork>absent(), Optional.<Future<?>> absent());
        }
      }
    } else {
      SynchronousFork fork = closer.register(
          new SynchronousFork(this.taskContext, schema instanceof Copyable ? ((Copyable) schema).copy() : schema,
              branches, 0, this.taskMode));
      configureStreamingFork(fork, watermarkingStrategy);
      this.forks.put(Optional.<Fork>of(fork), Optional.<Future<?>> of(this.taskExecutor.submit(fork)));
    }

    if (isStreamingTask()) {

      // Start watermark manager and tracker
      if (this.watermarkTracker.isPresent()) {
        this.watermarkTracker.get().start();
      }
      this.watermarkManager.get().start();

      ((StreamingExtractor) this.taskContext.getRawSourceExtractor()).start(this.watermarkStorage.get());


      RecordEnvelope recordEnvelope;
      // Extract, convert, and fork one source record at a time.
      while (!shutdownRequested() && (recordEnvelope = extractor.readRecordEnvelope()) != null) {
        onRecordExtract();
        AcknowledgableWatermark ackableWatermark = new AcknowledgableWatermark(recordEnvelope.getWatermark());
        if (watermarkTracker.isPresent()) {
          watermarkTracker.get().track(ackableWatermark);
        }
        for (Object convertedRecord : converter.convertRecord(schema, recordEnvelope, this.taskState)) {
          processRecord(convertedRecord, forkOperator, rowChecker, rowResults, branches,
              ackableWatermark.incrementAck());
        }
        ackableWatermark.ack();
      }
    } else {
      RecordEnvelope record;
      // Extract, convert, and fork one source record at a time.
      while ((record = extractor.readRecordEnvelope()) != null) {
        onRecordExtract();
        for (Object convertedRecord : converter.convertRecord(schema, record.getRecord(), this.taskState)) {
          processRecord(convertedRecord, forkOperator, rowChecker, rowResults, branches, null);
        }
      }
    }

    LOG.info("Extracted " + this.recordsPulled + " data records");
    LOG.info("Row quality checker finished with results: " + rowResults.getResults());

    this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXTRACTED, this.recordsPulled);
    this.taskState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, extractor.getExpectedRecordCount());

    for (Optional<Fork> fork : this.forks.keySet()) {
      if (fork.isPresent()) {
        // Tell the fork that the main branch is completed and no new incoming data records should be expected
        fork.get().markParentTaskDone();
      }
    }

    for (Optional<Future<?>> forkFuture : this.forks.values()) {
      if (forkFuture.isPresent()) {
        try {
          long forkFutureStartTime = System.nanoTime();
          forkFuture.get().get();
          long forkDuration = System.nanoTime() - forkFutureStartTime;
          LOG.info("Task shutdown: Fork future reaped in {} millis", forkDuration / 1000000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  protected void configureStreamingFork(Fork fork, String watermarkingStrategy) throws IOException {
    if (isStreamingTask()) {
      DataWriter forkWriter = fork.getWriter();
      if (forkWriter instanceof WatermarkAwareWriter) {
        if (watermarkingStrategy.equals("WriterBased")) {
          ((MultiWriterWatermarkManager) this.watermarkManager.get()).registerWriter((WatermarkAwareWriter) forkWriter);
        }
      } else {
        String errorMessage = String.format("The Task is configured to run in continuous mode, "
            + "but the writer %s is not a WatermarkAwareWriter", forkWriter.getClass().getName());
        LOG.error(errorMessage);
        throw new RuntimeException(errorMessage);
      }
    }
  }

  protected void onRecordExtract() {
    this.recordsPulled.incrementAndGet();
    this.lastRecordPulledTimestampMillis = System.currentTimeMillis();
  }

  private void failTask(Throwable t) {
    LOG.error(String.format("Task %s failed", this.taskId), t);
    this.taskState.setWorkingState(WorkUnitState.WorkingState.FAILED);
    this.taskState.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY, Throwables.getStackTraceAsString(t));
  }

  /**
   * Whether the task should directly publish its output data to the final publisher output directory.
   *
   * <p>
   *   The task should publish its output data directly if {@link ConfigurationKeys#PUBLISH_DATA_AT_JOB_LEVEL}
   *   is set to false AND any of the following conditions is satisfied:
   *
   *   <ul>
   *     <li>The {@link JobCommitPolicy#COMMIT_ON_PARTIAL_SUCCESS} policy is used.</li>
   *     <li>The {@link JobCommitPolicy#COMMIT_SUCCESSFUL_TASKS} policy is used. and all {@link Fork}s of this
   *     {@link Task} succeeded.</li>
   *   </ul>
   * </p>
   */
  private boolean shouldPublishDataInTask() {
    boolean publishDataAtJobLevel = this.taskState.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
        ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL);
    if (publishDataAtJobLevel) {
      LOG.info(String
          .format("%s is true. Will publish data at the job level.", ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL));
      return false;
    }

    JobCommitPolicy jobCommitPolicy = JobCommitPolicy.getCommitPolicy(this.taskState);

    if (jobCommitPolicy == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS) {
      return this.taskState.getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL;
    }

    if (jobCommitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS) {
      return true;
    }

    LOG.info("Will publish data at the job level with job commit policy: " + jobCommitPolicy);
    return false;
  }

  private void publishTaskData()
      throws IOException {
    Closer closer = Closer.create();
    try {
      Class<? extends DataPublisher> dataPublisherClass = getTaskPublisherClass();
      SingleTaskDataPublisher publisher =
          closer.register(SingleTaskDataPublisher.getInstance(dataPublisherClass, this.taskState));

      LOG.info("Publishing data from task " + this.taskId);
      publisher.publish(this.taskState);
    } catch (ClassCastException e) {
      LOG.error(String.format("To publish data in task, the publisher class must extend %s",
          SingleTaskDataPublisher.class.getSimpleName()), e);
      this.taskState.setTaskFailureException(e);
      throw closer.rethrow(e);
    } catch (Throwable t) {
      this.taskState.setTaskFailureException(t);
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @SuppressWarnings("unchecked")
  private Class<? extends DataPublisher> getTaskPublisherClass()
      throws ReflectiveOperationException {
    if (this.taskState.contains(ConfigurationKeys.TASK_DATA_PUBLISHER_TYPE)) {
      return (Class<? extends DataPublisher>) Class
          .forName(this.taskState.getProp(ConfigurationKeys.TASK_DATA_PUBLISHER_TYPE));
    }
    return (Class<? extends DataPublisher>) Class.forName(
        this.taskState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE, ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
  }

  /** Get the ID of the job this {@link Task} belongs to.
   *
   * @return ID of the job this {@link Task} belongs to.
   */
  public String getJobId() {
    return this.jobId;
  }

  /**
   * Get the ID of this task.
   *
   * @return ID of this task
   */
  public String getTaskId() {
    return this.taskId;
  }

  /**
   * Get the key of this task.
   *
   * @return Key of this task
   */
  public String getTaskKey() {
    return this.taskKey;
  }

  /**
   * Get the {@link TaskContext} associated with this task.
   *
   * @return {@link TaskContext} associated with this task
   */
  public TaskContext getTaskContext() {
    return this.taskContext;
  }

  /**
   * Get the state of this task.
   *
   * @return state of this task
   */
  public TaskState getTaskState() {
    return this.taskState;
  }

  @Override
  public State getPersistentState() {
    return getTaskState();
  }

  @Override
  public State getExecutionMetadata() {
    return getTaskState();
  }

  @Override
  public WorkUnitState.WorkingState getWorkingState() {
    return getTaskState().getWorkingState();
  }

  /**
   * Get the list of {@link Fork}s created by this {@link Task}.
   *
   * @return the list of {@link Fork}s created by this {@link Task}
   */
  public List<Optional<Fork>> getForks() {
    return ImmutableList.copyOf(this.forks.keySet());
  }

  /**
   * Update record-level metrics.
   */
  public void updateRecordMetrics() {
    for (Optional<Fork> fork : this.forks.keySet()) {
      if (fork.isPresent()) {
        fork.get().updateRecordMetrics();
      }
    }
  }

  /**
   * Update byte-level metrics.
   *
   * <p>
   *     This method is only supposed to be called after the writer commits.
   * </p>
   */
  public void updateByteMetrics() {
    try {
      for (Optional<Fork> fork : this.forks.keySet()) {
        if (fork.isPresent()) {
          fork.get().updateByteMetrics();
        }
      }
    } catch (IOException ioe) {
      LOG.error("Failed to update byte-level metrics for task " + this.taskId, ioe);
    }
  }

  /**
   * Increment the retry count of this task.
   */
  public void incrementRetryCount() {
    this.retryCount.incrementAndGet();
  }

  /**
   * Get the number of times this task has been retried.
   *
   * @return number of times this task has been retried
   */
  public int getRetryCount() {
    return this.retryCount.get();
  }

  /**
   * Mark the completion of this {@link Task}.
   */
  public void markTaskCompletion() {
    if (this.countDownLatch.isPresent()) {
      this.countDownLatch.get().countDown();
    }

    this.taskState.setProp(ConfigurationKeys.TASK_RETRIES_KEY, this.retryCount.get());
  }

  @Override
  public String toString() {
    return this.taskId;
  }

  /**
   * Process a (possibly converted) record.
   */
  @SuppressWarnings("unchecked")
  private void processRecord(Object convertedRecord, ForkOperator forkOperator, RowLevelPolicyChecker rowChecker,
      RowLevelPolicyCheckResults rowResults, int branches, AcknowledgableWatermark watermark)
      throws Exception {
    // Skip the record if quality checking fails
    if (!rowChecker.executePolicies(convertedRecord, rowResults)) {
      if (watermark != null) {
        watermark.ack();
      }
      return;
    }

    List<Boolean> forkedRecords = forkOperator.forkDataRecord(this.taskState, convertedRecord);
    if (forkedRecords.size() != branches) {
      throw new ForkBranchMismatchException(String
          .format("Number of forked data records [%d] is not equal to number of branches [%d]", forkedRecords.size(),
              branches));
    }

    boolean needToCopy = inMultipleBranches(forkedRecords);
    // we only have to copy a record if it needs to go into multiple forks
    if (needToCopy && !(CopyHelper.isCopyable(convertedRecord))) {
      throw new CopyNotSupportedException(convertedRecord.getClass().getName() + " is not copyable");
    }

    int branch = 0;
    int copyInstance = 0;
    for (Optional<Fork> fork : this.forks.keySet()) {
      if (fork.isPresent() && forkedRecords.get(branch)) {
        Object recordForFork = needToCopy ? CopyHelper.copy(convertedRecord) : convertedRecord;
        copyInstance++;
        if (isStreamingTask()) {
          // Send the record, watermark pair down the fork
          recordForFork = ((RecordEnvelope) recordForFork).withAckableWatermark(watermark.incrementAck());
        }
        // Put the record into the record queue of each fork. A put may timeout and return a false, in which
        // case the put is retried until it is successful.
        boolean succeeded = false;
        while (!succeeded) {
          succeeded = fork.get().putRecord(recordForFork);
        }
      }
      branch++;
    }
    if (watermark != null) {
      watermark.ack();
    }
  }

  /**
   * Check if a schema or data record is being passed to more than one branches.
   */
  private static boolean inMultipleBranches(List<Boolean> branches) {
    int inBranches = 0;
    for (Boolean bool : branches) {
      if (bool && ++inBranches > 1) {
        break;
      }
    }
    return inBranches > 1;
  }

  /**
   * Get the total number of records written by every {@link Fork}s of this {@link Task}.
   *
   * @return the number of records written by every {@link Fork}s of this {@link Task}
   */
  private long getRecordsWritten() {
    long recordsWritten = 0;
    for (Optional<Fork> fork : this.forks.keySet()) {
      recordsWritten += fork.get().getRecordsWritten();
    }
    return recordsWritten;
  }

  /**
   * Get the total number of bytes written by every {@link Fork}s of this {@link Task}.
   *
   * @return the number of bytes written by every {@link Fork}s of this {@link Task}
   */
  private long getBytesWritten() {
    long bytesWritten = 0;
    for (Optional<Fork> fork : this.forks.keySet()) {
      bytesWritten += fork.get().getBytesWritten();
    }
    return bytesWritten;
  }

  /**
   * Get the final state of each construct used by this task and add it to the {@link gobblin.runtime.TaskState}.
   * @param extractor the {@link gobblin.instrumented.extractor.InstrumentedExtractorBase} used by this task.
   * @param converter the {@link gobblin.converter.Converter} used by this task.
   * @param rowChecker the {@link RowLevelPolicyChecker} used by this task.
   */
  private void addConstructsFinalStateToTaskState(InstrumentedExtractorBase<?, ?> extractor,
      Converter<?, ?, ?, ?> converter, RowLevelPolicyChecker rowChecker) {
    ConstructState constructState = new ConstructState();
    if (extractor != null) {
      constructState.addConstructState(Constructs.EXTRACTOR, new ConstructState(extractor.getFinalState()));
    }
    if (converter != null) {
      constructState.addConstructState(Constructs.CONVERTER, new ConstructState(converter.getFinalState()));
    }
    if (rowChecker != null) {
      constructState.addConstructState(Constructs.ROW_QUALITY_CHECKER, new ConstructState(rowChecker.getFinalState()));
    }
    int forkIdx = 0;
    for (Optional<Fork> fork : this.forks.keySet()) {
      constructState.addConstructState(Constructs.FORK_OPERATOR, new ConstructState(fork.get().getFinalState()),
          Integer.toString(forkIdx));
      forkIdx++;
    }

    constructState.mergeIntoWorkUnitState(this.taskState);
  }

  /**
   * Commit this task by doing the following things:
   * 1. Committing each fork by {@link Fork#commit()}.
   * 2. Update final state of construct in {@link #taskState}.
   * 3. Check whether to publish data in task.
   */
  public void commit() {
    try {
      // Check if all forks succeeded
      List<Integer> failedForkIds = new ArrayList<>();
      for (Optional<Fork> fork : this.forks.keySet()) {
        if (fork.isPresent()) {
          if (fork.get().isSucceeded()) {
            if (!fork.get().commit()) {
              failedForkIds.add(fork.get().getIndex());
            }
          } else {
            failedForkIds.add(fork.get().getIndex());
          }
        }
      }

      if (failedForkIds.size() == 0) {
        // Set the task state to SUCCESSFUL. The state is not set to COMMITTED
        // as the data publisher will do that upon successful data publishing.
        this.taskState.setWorkingState(WorkUnitState.WorkingState.SUCCESSFUL);
      } else {
        failTask(new ForkException("Fork branches " + failedForkIds + " failed for task " + this.taskId));
      }
    } catch (Throwable t) {
      failTask(t);
    } finally {
      addConstructsFinalStateToTaskState(extractor, converter, rowChecker);

      this.taskState.setProp(ConfigurationKeys.WRITER_RECORDS_WRITTEN, getRecordsWritten());
      this.taskState.setProp(ConfigurationKeys.WRITER_BYTES_WRITTEN, getBytesWritten());

      this.submitTaskCommittedEvent();

      try {
        closer.close();
      } catch (Throwable t) {
        LOG.error("Failed to close all open resources", t);
      }

      for (Map.Entry<Optional<Fork>, Optional<Future<?>>> forkAndFuture : this.forks.entrySet()) {
        if (forkAndFuture.getKey().isPresent() && forkAndFuture.getValue().isPresent()) {
          try {
            forkAndFuture.getValue().get().cancel(true);
          } catch (Throwable t) {
            LOG.error(String.format("Failed to cancel Fork \"%s\"", forkAndFuture.getKey().get()), t);
          }
        }
      }

      try {
        if (shouldPublishDataInTask()) {
          // If data should be published by the task, publish the data and set the task state to COMMITTED.
          // Task data can only be published after all forks have been closed by closer.close().
          publishTaskData();
          this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
        }
      } catch (IOException ioe) {
        failTask(ioe);
      } finally {
        long endTime = System.currentTimeMillis();
        this.taskState.setEndTime(endTime);
        this.taskState.setTaskDuration(endTime - startTime);
        this.taskStateTracker.onTaskCommitCompletion(this);
      }
    }
  }

  protected void submitTaskCommittedEvent() {
    MetricContext taskMetricContext = TaskMetrics.get(this.taskState).getMetricContext();
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(taskMetricContext, "gobblin.runtime.task").build();
    eventSubmitter.submit(TaskEvent.TASK_COMMITTED_EVENT_NAME, ImmutableMap
        .of(TaskEvent.METADATA_TASK_ID, this.taskId, TaskEvent.METADATA_TASK_ATTEMPT_ID,
            this.taskState.getTaskAttemptId().or("")));
  }

  /**
   * @return true if the current {@link Task} is safe to have duplicate attempts; false, otherwise.
   */
  public boolean isSpeculativeExecutionSafe() {
    if (this.extractor instanceof SpeculativeAttemptAwareConstruct) {
      if (!((SpeculativeAttemptAwareConstruct) this.extractor).isSpeculativeAttemptSafe()) {
        return false;
      }
    }

    if (this.converter instanceof SpeculativeAttemptAwareConstruct) {
      if (!((SpeculativeAttemptAwareConstruct) this.converter).isSpeculativeAttemptSafe()) {
        return false;
      }
    }

    for (Optional<Fork> fork : this.forks.keySet()) {
      if (fork.isPresent() && !fork.get().isSpeculativeExecutionSafe()) {
        return false;
      }
    }
    return true;
  }
}
