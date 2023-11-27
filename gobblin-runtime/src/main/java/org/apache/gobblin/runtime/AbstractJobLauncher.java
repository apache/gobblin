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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Authenticator;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.github.rholder.retry.RetryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.CommitSequence;
import org.apache.gobblin.commit.CommitSequenceStore;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.initializer.ConverterInitializerFactory;
import org.apache.gobblin.destination.DestinationDatasetHandlerService;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.GobblinMetricsRegistry;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventName;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.MultiEventMetadataGenerator;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.job_spec.JobSpecResolver;
import org.apache.gobblin.runtime.listeners.CloseableJobListener;
import org.apache.gobblin.runtime.listeners.JobExecutionEventSubmitterListener;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.listeners.JobListeners;
import org.apache.gobblin.runtime.locks.JobLock;
import org.apache.gobblin.runtime.locks.JobLockEventListener;
import org.apache.gobblin.runtime.locks.JobLockException;
import org.apache.gobblin.runtime.locks.LegacyJobLockFactoryManager;
import org.apache.gobblin.runtime.metrics.GobblinJobMetricReporter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooterFactory;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.runtime.util.JobMetrics;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.InfiniteSource;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.WorkUnitStreamSource;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.stream.WorkUnitChangeEvent;
import org.apache.gobblin.util.ClusterNameTags;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.Id;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.initializer.WriterInitializerFactory;



/**
 * An abstract implementation of {@link JobLauncher} that handles common tasks for launching and running a job.
 *
 * @author Yinan Li
 */
public abstract class AbstractJobLauncher implements JobLauncher {

  static final Logger LOG = LoggerFactory.getLogger(AbstractJobLauncher.class);

  public static final String TASK_STATE_STORE_TABLE_SUFFIX = ".tst";

  public static final String JOB_STATE_FILE_NAME = "job.state";

  public static final String GOBBLIN_JOB_TEMPLATE_KEY = "gobblin.template.uri";

  public static final String NUM_WORKUNITS = "numWorkUnits";

  /** Making {@link AbstractJobLauncher} capable of loading multiple job templates.
   * Keep the original {@link #GOBBLIN_JOB_TEMPLATE_KEY} for backward-compatibility.
   * TODO: Expand support to Gobblin-as-a-Service in FlowTemplateCatalog.
   * */
  public static final String GOBBLIN_JOB_MULTI_TEMPLATE_KEY = "gobblin.template.uris";

  // Job configuration properties
  protected final Properties jobProps;

  // This contains all job context information
  protected final JobContext jobContext;

  // This (optional) JobLock is used to prevent the next scheduled run
  // of the job from starting if the current run has not finished yet
  protected Optional<JobLock> jobLockOptional = Optional.absent();

  // A conditional variable for which the condition is satisfied if a cancellation is requested
  protected final Object cancellationRequest = new Object();

  // A flag indicating whether a cancellation has been requested or not
  protected volatile boolean cancellationRequested = false;

  // A conditional variable for which the condition is satisfied if the cancellation is executed
  protected final Object cancellationExecution = new Object();

  // A flag indicating whether a cancellation has been executed or not
  protected volatile boolean cancellationExecuted = false;

  // A single-thread executor for executing job cancellation
  protected final ExecutorService cancellationExecutor;

  // An MetricContext to track runtime metrics only if metrics are enabled.
  protected final Optional<MetricContext> runtimeMetricContext;

  // An EventBuilder with basic metadata.
  protected final EventSubmitter eventSubmitter;

  // This is for dispatching events related to job launching and execution to registered subscribers
  protected final EventBus eventBus = new EventBus(AbstractJobLauncher.class.getSimpleName());

  // A list of JobListeners that will be injected into the user provided JobListener
  private final List<JobListener> mandatoryJobListeners = Lists.newArrayList();

  // Used to generate additional metadata to emit in timing events
  private final MultiEventMetadataGenerator multiEventMetadataGenerator;

  private final AutomaticTroubleshooter troubleshooter;

  protected final GobblinJobMetricReporter gobblinJobMetricsReporter;

  public AbstractJobLauncher(Properties jobProps, List<? extends Tag<?>> metadataTags)
      throws Exception {
    this(jobProps, metadataTags, null);
  }

  public AbstractJobLauncher(Properties jobProps, List<? extends Tag<?>> metadataTags,
      @Nullable SharedResourcesBroker<GobblinScopeTypes> instanceBroker)
      throws Exception {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");

    // Add clusterIdentifier tag so that it is added to any new TaskState created
    List<Tag<?>> clusterNameTags = Lists.newArrayList();
    clusterNameTags.addAll(Tag.fromMap(ClusterNameTags.getClusterNameTags()));
    GobblinMetrics.addCustomTagsToProperties(jobProps, clusterNameTags);

    troubleshooter = AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(jobProps));
    troubleshooter.start();

    // Make a copy for both the system and job configuration properties and resolve the job-template if any.
    this.jobProps = new Properties();
    this.jobProps.putAll(jobProps);
    resolveGobblinJobTemplateIfNecessary(this.jobProps);

    if (!tryLockJob(this.jobProps)) {
      throw new JobException(String.format("Previous instance of job %s is still running, skipping this scheduled run",
          this.jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)));
    }

    try {
      setDefaultAuthenticator(this.jobProps);

      if (instanceBroker == null) {
        instanceBroker = createDefaultInstanceBroker(jobProps);
      }

      this.jobContext = new JobContext(this.jobProps, LOG, instanceBroker, troubleshooter.getIssueRepository());
      this.eventBus.register(this.jobContext);

      this.cancellationExecutor = Executors.newSingleThreadExecutor(
          ExecutorsUtils.newDaemonThreadFactory(Optional.of(LOG), Optional.of("CancellationExecutor")));

      this.runtimeMetricContext =
          this.jobContext.getJobMetricsOptional().transform(new Function<JobMetrics, MetricContext>() {
            @Override
            public MetricContext apply(JobMetrics input) {
              return input.getMetricContext();
            }
          });

      this.eventSubmitter = buildEventSubmitter(metadataTags);
      // Add all custom tags to the JobState so that tags are added to any new TaskState created
      GobblinMetrics.addCustomTagToState(this.jobContext.getJobState(), metadataTags);

      JobExecutionEventSubmitter jobExecutionEventSubmitter = new JobExecutionEventSubmitter(this.eventSubmitter);
      this.mandatoryJobListeners.add(new JobExecutionEventSubmitterListener(jobExecutionEventSubmitter));

      this.multiEventMetadataGenerator = new MultiEventMetadataGenerator(
          PropertiesUtils.getPropAsList(jobProps, ConfigurationKeys.EVENT_METADATA_GENERATOR_CLASS_KEY,
              ConfigurationKeys.DEFAULT_EVENT_METADATA_GENERATOR_CLASS_KEY));

      String jobMetricsReporterClassName = this.jobProps.getProperty(ConfigurationKeys.JOB_METRICS_REPORTER_CLASS_KEY, ConfigurationKeys.DEFAULT_JOB_METRICS_REPORTER_CLASS);
      this.gobblinJobMetricsReporter =  (GobblinJobMetricReporter) GobblinConstructorUtils.invokeLongestConstructor(Class.forName(jobMetricsReporterClassName),
          this.runtimeMetricContext);

    } catch (Exception e) {
      unlockJob();
      throw e;
    }
  }

  /**
   * Set default {@link Authenticator} to the one provided in {@link ConfigurationKeys#DEFAULT_AUTHENTICATOR_CLASS},
   * calling the constructor using the provided {@link Properties}
   */
  public static void setDefaultAuthenticator(Properties properties) {
    String authenticatorClass = properties.getProperty(ConfigurationKeys.DEFAULT_AUTHENTICATOR_CLASS);
    if (!Strings.isNullOrEmpty(authenticatorClass)) {
      Authenticator authenticator = GobblinConstructorUtils.invokeConstructor(Authenticator.class, authenticatorClass, properties);
      Authenticator.setDefault(authenticator);
    }
  }

  /**
   * Handle {@link WorkUnitChangeEvent}, by default it will do nothing
   */
  @Subscribe
  public void handleWorkUnitChangeEvent(WorkUnitChangeEvent workUnitChangeEvent)
      throws InvocationTargetException {
    LOG.info("start to handle workunit change event");
    try {
      this.removeTasksFromCurrentJob(workUnitChangeEvent.getOldTaskIds());
      this.addTasksToCurrentJob(workUnitChangeEvent.getNewWorkUnits());
    } catch (Exception e) {
      //todo: emit some event to indicate there is an error handling this event that may cause starvation
      throw new InvocationTargetException(e);
    }
  }

  protected void removeTasksFromCurrentJob(List<String> taskIdsToRemove) throws IOException, ExecutionException,
                                                                                RetryException {}
  protected void addTasksToCurrentJob(List<WorkUnit> workUnitsToAdd) throws IOException, ExecutionException,
                                                                            RetryException {}

  /**
   * To supporting 'gobblin.template.uri' in any types of jobLauncher, place this resolution as a public-static method
   * to make it accessible for all implementation of JobLauncher and **AzkabanJobLauncher**.
   *
   * @param jobProps Gobblin Job-level properties.
   */
  public static void resolveGobblinJobTemplateIfNecessary(Properties jobProps) throws IOException, URISyntaxException,
                                                                                      SpecNotFoundException,
                                                                                      JobTemplate.TemplateException {
    Config config = ConfigUtils.propertiesToConfig(jobProps);
    JobSpecResolver resolver = JobSpecResolver.builder(config).build();
    JobSpec jobSpec = null;
    if (jobProps.containsKey(GOBBLIN_JOB_TEMPLATE_KEY)) {
      URI templateUri = new URI(jobProps.getProperty(GOBBLIN_JOB_TEMPLATE_KEY));
      jobSpec = JobSpec.builder().withConfig(config).withTemplate(templateUri).build();
    } else if (jobProps.containsKey(GOBBLIN_JOB_MULTI_TEMPLATE_KEY)) {
      List<URI> templatesURIs = new ArrayList<>();
      for (String uri : jobProps.getProperty(GOBBLIN_JOB_MULTI_TEMPLATE_KEY).split(",")) {
        templatesURIs.add(new URI(uri));
      }
      jobSpec = JobSpec.builder().withConfig(config).withResourceTemplates(templatesURIs).build();
    }
    if (jobSpec != null ) {
      jobProps.putAll(ConfigUtils.configToProperties(resolver.resolveJobSpec(jobSpec).getConfig()));
    }
  }

  private static SharedResourcesBroker<GobblinScopeTypes> createDefaultInstanceBroker(Properties jobProps) {
    LOG.warn("Creating a job specific {}. Objects will only be shared at the job level.",
        SharedResourcesBroker.class.getSimpleName());
    return SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.parseProperties(jobProps),
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
  }

  /**
   * The JobContext of the particular job.
   *
   * @return {@link JobContext} of the job
   */
  JobContext getJobContext() {
    return this.jobContext;
  }

  /**
   * A default implementation of {@link JobLauncher#cancelJob(JobListener)}.
   *
   * <p>
   *   This implementation relies on two conditional variables: one for the condition that a cancellation
   *   is requested, and the other for the condition that the cancellation is executed. Upon entrance, the
   *   method notifies the cancellation executor started by {@link #startCancellationExecutor()} on the
   *   first conditional variable to indicate that a cancellation has been requested so the executor is
   *   unblocked. Then it waits on the second conditional variable for the cancellation to be executed.
   * </p>
   *
   * <p>
   *   The actual execution of the cancellation is handled by the cancellation executor started by the
   *   method {@link #startCancellationExecutor()} that uses the {@link #executeCancellation()} method
   *   to execute the cancellation.
   * </p>
   *
   * {@inheritDoc JobLauncher#cancelJob(JobListener)}
   */
  @Override
  public void cancelJob(JobListener jobListener)
      throws JobException {
    synchronized (this.cancellationRequest) {
      if (this.cancellationRequested) {
        // Return immediately if a cancellation has already been requested
        return;
      }

      this.cancellationRequested = true;
      // Notify the cancellation executor that a cancellation has been requested
      this.cancellationRequest.notify();
    }

    synchronized (this.cancellationExecution) {
      try {
        while (!this.cancellationExecuted) {
          // Wait for the cancellation to be executed
          this.cancellationExecution.wait();
        }

        try {
          LOG.info("Current job state is: " + this.jobContext.getJobState().getState());
          if (this.jobContext.getJobState().getState() != JobState.RunningState.COMMITTED && (
              this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS
                  || this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS)) {
            this.jobContext.finalizeJobStateBeforeCommit();
            this.jobContext.commit(true);
          }
          this.jobContext.close();
        } catch (IOException ioe) {
          LOG.error("Could not close job context.", ioe);
        }
        notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_CANCEL, new JobListenerAction() {
          @Override
          public void apply(JobListener jobListener, JobContext jobContext)
              throws Exception {
            jobListener.onJobCancellation(jobContext);
          }
        });
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * This predicate checks if a work unit should be skipped. If yes, then it will removed
   * from the list of workUnits and it's state will be saved.
   */
  @RequiredArgsConstructor
  private static class SkippedWorkUnitsFilter implements Predicate<WorkUnit> {
    private final JobState jobState;

    @Override
    public boolean apply(WorkUnit workUnit) {
      if (workUnit instanceof MultiWorkUnit) {
        Preconditions.checkArgument(!workUnit.contains(ConfigurationKeys.WORK_UNIT_SKIP_KEY),
            "Error: MultiWorkUnit cannot be skipped");
        for (WorkUnit wu : ((MultiWorkUnit) workUnit).getWorkUnits()) {
          Preconditions.checkArgument(!wu.contains(ConfigurationKeys.WORK_UNIT_SKIP_KEY),
              "Error: MultiWorkUnit cannot contain skipped WorkUnit");
        }
      }
      if (workUnit.getPropAsBoolean(ConfigurationKeys.WORK_UNIT_SKIP_KEY, false)) {
        WorkUnitState workUnitState = new WorkUnitState(workUnit, this.jobState);
        workUnitState.setWorkingState(WorkUnitState.WorkingState.SKIPPED);
        this.jobState.addSkippedTaskState(new TaskState(workUnitState));
        return false;
      }
      return true;
    }
  }

  @Override
  public void launchJob(JobListener jobListener)
      throws JobException {
    String jobId = this.jobContext.getJobId();
    final JobState jobState = this.jobContext.getJobState();
    boolean isWorkUnitsEmpty = false;
    try {
      MDC.put(ConfigurationKeys.JOB_NAME_KEY, this.jobContext.getJobName());
      MDC.put(ConfigurationKeys.JOB_KEY_KEY, this.jobContext.getJobKey());
      TimingEvent launchJobTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.FULL_JOB_EXECUTION);

      try (Closer closer = Closer.create()) {
        closer.register(this.jobContext);
        notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_PREPARE, new JobListenerAction() {
          @Override
          public void apply(JobListener jobListener, JobContext jobContext)
              throws Exception {
            jobListener.onJobPrepare(jobContext);
          }
        });

        if (this.jobContext.getSemantics() == DeliverySemantics.EXACTLY_ONCE) {

          // If exactly-once is used, commit sequences of the previous run must be successfully compelted
          // before this run can make progress.
          executeUnfinishedCommitSequences(jobState.getJobName());
        }

        Source<?, ?> source = this.jobContext.getSource();
        if (source instanceof InfiniteSource) {
          ((InfiniteSource) source).getEventBus().register(this);
        } else if (source instanceof SourceDecorator) {
          if (((SourceDecorator<?, ?>) source).getEventBus() != null) {
            ((SourceDecorator<?, ?>) source).getEventBus().register(this);
          }
        }
        TimingEvent workUnitsCreationTimer =
            this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.WORK_UNITS_CREATION);
        WorkUnitStream workUnitStream;
        if (source instanceof WorkUnitStreamSource) {
          workUnitStream = ((WorkUnitStreamSource) source).getWorkunitStream(jobState);
        } else {
          workUnitStream = new BasicWorkUnitStream.Builder(source.getWorkunits(jobState)).build();
        }
        workUnitsCreationTimer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext,
            EventName.WORK_UNITS_CREATION));

        this.gobblinJobMetricsReporter.reportWorkUnitCreationTimerMetrics(workUnitsCreationTimer, jobState);
        // The absence means there is something wrong getting the work units
        if (workUnitStream == null || workUnitStream.getWorkUnits() == null) {
          this.eventSubmitter.submit(JobEvent.WORK_UNITS_MISSING);
          jobState.setState(JobState.RunningState.FAILED);
          String errMsg = "Failed to get work units for job " + jobId;
          this.jobContext.getJobState().setJobFailureMessage(errMsg);
          this.jobContext.getJobState().setProp(NUM_WORKUNITS, 0);
          this.gobblinJobMetricsReporter.reportWorkUnitCountMetrics(0, jobState);
          throw new JobException(errMsg);
        }

        // No work unit to run
        if (!workUnitStream.getWorkUnits().hasNext()) {
          this.eventSubmitter.submit(JobEvent.WORK_UNITS_EMPTY);
          LOG.warn("No work units have been created for job " + jobId);
          jobState.setState(JobState.RunningState.COMMITTED);
          isWorkUnitsEmpty = true;
          this.jobContext.getJobState().setProp(NUM_WORKUNITS, 0);
          this.gobblinJobMetricsReporter.reportWorkUnitCountMetrics(0, jobState);
          return;
        }

        // calculation of total bytes to copy in a job used to track a job's copy progress
        if (jobState.getPropAsBoolean(ConfigurationKeys.REPORT_JOB_PROGRESS, ConfigurationKeys.DEFAULT_REPORT_JOB_PROGRESS)) {
            LOG.info("Report job progress config is turned on");
          if (workUnitStream.isSafeToMaterialize()) {
            long totalSizeInBytes = sumWorkUnitsSizes(workUnitStream);
            this.jobContext.getJobState().setProp(ServiceConfigKeys.TOTAL_WORK_UNIT_SIZE, totalSizeInBytes);
          } else {
            LOG.warn("Property " + ConfigurationKeys.REPORT_JOB_PROGRESS + " is turned on, but "
                + "progress cannot be reported for infinite work unit streams. Turn off property "
                + ConfigurationKeys.REPORT_JOB_PROGRESS);
          }
        }

        // Perform work needed before writing is done
        Boolean canCleanUp = this.canCleanStagingData(this.jobContext.getJobState());
        workUnitStream = closer.register(new DestinationDatasetHandlerService(jobState, canCleanUp, this.eventSubmitter))
            .executeHandlers(workUnitStream);

        //Initialize writer and converter(s)
        closer.register(WriterInitializerFactory.newInstace(jobState, workUnitStream)).initialize();
        closer.register(ConverterInitializerFactory.newInstance(jobState, workUnitStream)).initialize();

        TimingEvent stagingDataCleanTimer =
            this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.MR_STAGING_DATA_CLEAN);
        // Cleanup left-over staging data possibly from the previous run. This is particularly
        // important if the current batch of WorkUnits include failed WorkUnits from the previous
        // run which may still have left-over staging data not cleaned up yet.
        cleanLeftoverStagingData(workUnitStream, jobState);
        stagingDataCleanTimer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext,
            EventName.MR_STAGING_DATA_CLEAN));

        long startTime = System.currentTimeMillis();
        jobState.setStartTime(startTime);
        jobState.setState(JobState.RunningState.RUNNING);

        try {
          LOG.info("Starting job " + jobId);

          notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_START, new JobListenerAction() {
            @Override
            public void apply(JobListener jobListener, JobContext jobContext)
                throws Exception {
              jobListener.onJobStart(jobContext);
            }
          });

          TimingEvent workUnitsPreparationTimer =
              this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.WORK_UNITS_PREPARATION);
          // Add task ids
          workUnitStream = prepareWorkUnits(workUnitStream, jobState);
          // Remove skipped workUnits from the list of work units to execute.
          workUnitStream = workUnitStream.filter(new SkippedWorkUnitsFilter(jobState));
          // Add surviving tasks to jobState
          workUnitStream = workUnitStream.transform(new MultiWorkUnitForEach() {
            @Override
            public void forWorkUnit(WorkUnit workUnit) {
              jobState.incrementTaskCount();
              jobState.addTaskState(new TaskState(new WorkUnitState(workUnit, jobState)));
            }
          });

          // If it is a streaming source, workunits cannot be counted
          this.jobContext.getJobState().setProp(NUM_WORKUNITS,
              workUnitStream.isSafeToMaterialize() ? workUnitStream.getMaterializedWorkUnitCollection().size() : 0);
          this.gobblinJobMetricsReporter.reportWorkUnitCountMetrics(this.jobContext.getJobState().getPropAsInt(NUM_WORKUNITS), jobState);
          // dump the work unit if tracking logs are enabled
          if (jobState.getPropAsBoolean(ConfigurationKeys.WORK_UNIT_ENABLE_TRACKING_LOGS)) {
            workUnitStream = workUnitStream.transform(new Function<WorkUnit, WorkUnit>() {
              @Nullable
              @Override
              public WorkUnit apply(@Nullable WorkUnit input) {
                LOG.info("Work unit tracking log: {}", input);
                return input;
              }
            });
          }

          workUnitsPreparationTimer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext,
              EventName.WORK_UNITS_PREPARATION));

          // Write job execution info to the job history store before the job starts to run
          this.jobContext.storeJobExecutionInfo();

          TimingEvent jobRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_RUN);
          // Start the job and wait for it to finish
          runWorkUnitStream(workUnitStream);
          jobRunTimer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext,EventName.JOB_RUN));

          this.eventSubmitter
              .submit(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, "JOB_" + jobState.getState()));

          // Check and set final job jobPropsState upon job completion
          if (jobState.getState() == JobState.RunningState.CANCELLED) {
            LOG.info(String.format("Job %s has been cancelled, aborting now", jobId));
            return;
          }

          TimingEvent jobCommitTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_COMMIT);
          this.jobContext.finalizeJobStateBeforeCommit();
          this.jobContext.commit();
          postProcessJobState(jobState);
          jobCommitTimer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext, EventName.JOB_COMMIT));
        } finally {
          long endTime = System.currentTimeMillis();
          jobState.setEndTime(endTime);
          jobState.setDuration(endTime - jobState.getStartTime());
        }
      } catch (Throwable t) {
        jobState.setState(JobState.RunningState.FAILED);
        String errMsg = "Failed to launch and run job " + jobId + " due to " + t.getMessage();
        LOG.error(errMsg + ": " + t, t);
        this.jobContext.getJobState().setJobFailureException(t);
        jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY,
            Integer.parseInt(jobState.getProp(ConfigurationKeys.JOB_FAILURES_KEY, "0")) + 1);
      } finally {
        try {
          troubleshooter.refineIssues();
          troubleshooter.logIssueSummary();
          troubleshooter.reportJobIssuesAsEvents(eventSubmitter);
        } catch (Exception e) {
          LOG.error("Failed to report issues", e);
        }

        try {
          TimingEvent jobCleanupTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_CLEANUP);
          cleanupStagingData(jobState);
          jobCleanupTimer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext, EventName.JOB_CLEANUP));
          // Write job execution info to the job history store upon job termination
          this.jobContext.storeJobExecutionInfo();
        } finally {
          launchJobTimer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext, EventName.FULL_JOB_EXECUTION));
          if (isWorkUnitsEmpty) {
            //If no WorkUnits are created, first send the JobCompleteTimer event.
            notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_COMPLETE, new JobListenerAction() {
              @Override
              public void apply(JobListener jobListener, JobContext jobContext)
                  throws Exception {
                jobListener.onJobCompletion(jobContext);
              }
            });
            //Next, send the JobSucceededTimer event.
            notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_SUCCEEDED, new JobListenerAction() {
              @Override
              public void apply(JobListener jobListener, JobContext jobContext)
                  throws Exception {
                jobListener.onJobFailure(jobContext);
              }
            });
          } else {
            for (JobState.DatasetState datasetState : this.jobContext.getDatasetStatesByUrns().values()) {
              // Set the overall job state to FAILED if the job failed to process any dataset
              if (datasetState.getState() == JobState.RunningState.FAILED) {
                jobState.setState(JobState.RunningState.FAILED);
                LOG.warn("At least one dataset state is FAILED. Setting job state to FAILED.");
                break;
              }
            }

            notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_COMPLETE, new JobListenerAction() {
              @Override
              public void apply(JobListener jobListener, JobContext jobContext)
                  throws Exception {
                jobListener.onJobCompletion(jobContext);
              }
            });

            if (jobState.getState() == JobState.RunningState.FAILED || jobState.getState() == JobState.RunningState.CANCELLED) {
              notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_FAILED, new JobListenerAction() {
                @Override
                public void apply(JobListener jobListener, JobContext jobContext)
                    throws Exception {
                  jobListener.onJobFailure(jobContext);
                }
              });
              throw new JobException(String.format("Job %s failed", jobId));
            } else {
              notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_SUCCEEDED, new JobListenerAction() {
                @Override
                public void apply(JobListener jobListener, JobContext jobContext)
                    throws Exception {
                  jobListener.onJobFailure(jobContext);
                }
              });
            }
          }
        }
      }
    } finally {
      // Stop metrics reporting
      if (this.jobContext.getJobMetricsOptional().isPresent()) {
        JobMetrics.remove(jobState);
      }
      MDC.remove(ConfigurationKeys.JOB_NAME_KEY);
      MDC.remove(ConfigurationKeys.JOB_KEY_KEY);
    }
  }

  @VisibleForTesting
  public static long sumWorkUnitsSizes (WorkUnitStream workUnitStream) {
    Collection<WorkUnit> workUnits = JobLauncherUtils.flattenWorkUnits(workUnitStream.getMaterializedWorkUnitCollection());
    long totalSizeInBytes = workUnits.stream().mapToLong(wu -> wu.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE, 0)).sum();
    return totalSizeInBytes;
  }

  private void executeUnfinishedCommitSequences(String jobName)
      throws IOException {
    Preconditions.checkState(this.jobContext.getCommitSequenceStore().isPresent());
    CommitSequenceStore commitSequenceStore = this.jobContext.getCommitSequenceStore().get();

    for (String datasetUrn : commitSequenceStore.get(jobName)) {
      Optional<CommitSequence> commitSequence = commitSequenceStore.get(jobName, datasetUrn);
      if (commitSequence.isPresent()) {
        commitSequence.get().execute();
      }
      commitSequenceStore.delete(jobName, datasetUrn);
    }
  }

  /**
   * Subclasses can override this method to do whatever processing on the {@link TaskState}s,
   * e.g., aggregate task-level metrics into job-level metrics.
   *
   * @deprecated Use {@link #postProcessJobState(JobState)}
   */
  @Deprecated
  protected void postProcessTaskStates(@SuppressWarnings("unused") List<TaskState> taskStates) {
    // Do nothing
  }

  /**
   * Subclasses can override this method to do whatever processing on the {@link JobState} and its
   * associated {@link TaskState}s, e.g., aggregate task-level metrics into job-level metrics.
   */
  protected void postProcessJobState(JobState jobState) {
    postProcessTaskStates(jobState.getTaskStates());
    if (!GobblinMetrics.isEnabled(this.jobProps)) {
      return;
    }
    List<DatasetTaskSummary> datasetTaskSummaries = new ArrayList<>();
    Map<String, JobState.DatasetState> datasetStates = this.jobContext.getDatasetStatesByUrns();
    // Only process successful datasets unless configuration to process failed datasets is set
    boolean processFailedTasks = PropertiesUtils.getPropAsBoolean(this.jobProps, ConfigurationKeys.WRITER_COUNT_METRICS_FROM_FAILED_TASKS, "false");
    for (JobState.DatasetState datasetState : datasetStates.values()) {
      if (datasetState.getState() == JobState.RunningState.COMMITTED
        || (datasetState.getState() == JobState.RunningState.FAILED && this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS)) {
        long totalBytesWritten = 0;
        long totalRecordsWritten = 0;
        for (TaskState taskState : datasetState.getTaskStates()) {
          // Certain writers may omit these metrics e.g. CompactionLauncherWriter
          if ((taskState.getWorkingState() == WorkUnitState.WorkingState.COMMITTED || processFailedTasks)) {
            totalBytesWritten += taskState.getPropAsLong(ConfigurationKeys.WRITER_BYTES_WRITTEN, 0);
            totalRecordsWritten += taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN, 0);
          }
        }
        LOG.info(String.format("DatasetMetrics for '%s' - (records: %d; bytes: %d)", datasetState.getDatasetUrn(), totalRecordsWritten, totalBytesWritten));
        datasetTaskSummaries.add(new DatasetTaskSummary(datasetState.getDatasetUrn(), totalRecordsWritten, totalBytesWritten,
            datasetState.getState() == JobState.RunningState.COMMITTED));
      } else if (datasetState.getState() == JobState.RunningState.FAILED) {
        // Check if config is turned on for submitting writer metrics on failure due to non-atomic write semantics
        if (this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
          LOG.info("Due to task failure, will report that no records or bytes were written for " + datasetState.getDatasetUrn());
          datasetTaskSummaries.add(new DatasetTaskSummary(datasetState.getDatasetUrn(), 0, 0, false));
        }
      }
    }
    TimingEvent jobSummaryTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_SUMMARY);
    jobSummaryTimer.addMetadata(TimingEvent.DATASET_TASK_SUMMARIES, GsonUtils.GSON_WITH_DATE_HANDLING.toJson(datasetTaskSummaries));
    jobSummaryTimer.stop();
  }

  @Override
  public void close()
      throws IOException {

    troubleshooter.stop();

    try {
      this.cancellationExecutor.shutdownNow();
      try {
        this.jobContext.getSource().shutdown(this.jobContext.getJobState());
      } finally {
        if (GobblinMetrics.isEnabled(this.jobProps)) {
          GobblinMetricsRegistry.getInstance().remove(this.jobContext.getJobId());
        }
      }
    } finally {
      unlockJob();
    }
  }

  /**
   * Run the given job.
   *
   * <p>
   *   The contract between {@link AbstractJobLauncher#launchJob(JobListener)} and this method is this method
   *   is responsible for for setting {@link JobState.RunningState} properly and upon returning from this method
   *   (either normally or due to exceptions) whatever {@link JobState.RunningState} is set in this method is
   *   used to determine if the job has finished.
   * </p>
   *
   * @param workUnits List of {@link WorkUnit}s of the job
   */
  protected abstract void runWorkUnits(List<WorkUnit> workUnits)
      throws Exception;

  /**
   * Run the given job.
   *
   * <p>
   *   The contract between {@link AbstractJobLauncher#launchJob(JobListener)} and this method is this method
   *   is responsible for for setting {@link JobState.RunningState} properly and upon returning from this method
   *   (either normally or due to exceptions) whatever {@link JobState.RunningState} is set in this method is
   *   used to determine if the job has finished.
   * </p>
   *
   * @param workUnitStream stream of {@link WorkUnit}s of the job
   */
  protected void runWorkUnitStream(WorkUnitStream workUnitStream) throws Exception {
    runWorkUnits(materializeWorkUnitList(workUnitStream));
  }

  /**
   * Materialize a {@link WorkUnitStream} into an in-memory list. Note that infinite work unit streams cannot be materialized.
   */
  private List<WorkUnit> materializeWorkUnitList(WorkUnitStream workUnitStream) {
    if (!workUnitStream.isFiniteStream()) {
      throw new UnsupportedOperationException("Cannot materialize an infinite work unit stream.");
    }
    return Lists.newArrayList(workUnitStream.getWorkUnits());
  }

  /**
   * Get a {@link JobLock} to be used for the job.
   *
   * @param properties the job properties
   * @param jobLockEventListener the listener for lock events.
   * @return {@link JobLock} to be used for the job
   * @throws JobLockException throw when the {@link JobLock} fails to initialize
   */
  protected JobLock getJobLock(Properties properties, JobLockEventListener jobLockEventListener)
      throws JobLockException {
    return LegacyJobLockFactoryManager.getJobLock(properties, jobLockEventListener);
  }

  /**
   * Execute the job cancellation.
   * The implementation should not throw any exceptions because that will kill the `Cancellation Executor` thread
   * and will create a deadlock.
   */
  protected abstract void executeCancellation();

  /**
   * Start the scheduled executor for executing job cancellation.
   *
   * <p>
   *   The executor, upon started, waits on the condition variable indicating a cancellation is requested,
   *   i.e., it waits for a cancellation request to arrive. If a cancellation is requested, the executor
   *   is unblocked and calls {@link #executeCancellation()} to execute the cancellation. Upon completion
   *   of the cancellation execution, the executor notifies the caller that requested the cancellation on
   *   the conditional variable indicating the cancellation has been executed so the caller is unblocked.
   *   Upon successful execution of the cancellation, it sets the job state to
   *   {@link JobState.RunningState#CANCELLED}.
   * </p>
   */
  protected void startCancellationExecutor() {
    this.cancellationExecutor.execute(new Runnable() {
      @Override
      public void run() {
        synchronized (AbstractJobLauncher.this.cancellationRequest) {
          try {
            while (!AbstractJobLauncher.this.cancellationRequested) {
              // Wait for a cancellation request to arrive
              AbstractJobLauncher.this.cancellationRequest.wait();
            }
            LOG.info("Cancellation has been requested for job " + AbstractJobLauncher.this.jobContext.getJobId());
            executeCancellation();
            LOG.info("Cancellation has been executed for job " + AbstractJobLauncher.this.jobContext.getJobId());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }

        synchronized (AbstractJobLauncher.this.cancellationExecution) {
          AbstractJobLauncher.this.cancellationExecuted = true;
          AbstractJobLauncher.this.jobContext.getJobState().setState(JobState.RunningState.CANCELLED);
          // Notify that the cancellation has been executed
          AbstractJobLauncher.this.cancellationExecution.notifyAll();
        }
      }
    });
  }

  /**
   * Prepare the flattened {@link WorkUnit}s for execution by populating the job and task IDs.
   */
  private WorkUnitStream prepareWorkUnits(WorkUnitStream workUnits, JobState jobState) {
    return workUnits.transform(new WorkUnitPreparator(this.jobContext.getJobId()));
  }

  private static abstract class MultiWorkUnitForEach implements Function<WorkUnit, WorkUnit> {

    @Nullable
    @Override
    public WorkUnit apply(WorkUnit input) {
      if (input instanceof MultiWorkUnit) {
        for (WorkUnit wu : ((MultiWorkUnit) input).getWorkUnits()) {
          forWorkUnit(wu);
        }
      } else {
        forWorkUnit(input);
      }
      return input;
    }

    protected abstract void forWorkUnit(WorkUnit workUnit);
  }

  @RequiredArgsConstructor
  private static class WorkUnitPreparator extends MultiWorkUnitForEach {
    private int taskIdSequence = 0;
    private final String jobId;

    @Override
    protected void forWorkUnit(WorkUnit workUnit) {
      workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, this.jobId);
      String taskId = JobLauncherUtils.newTaskId(this.jobId, this.taskIdSequence++);
      workUnit.setId(taskId);
      workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
      workUnit.setProp(ConfigurationKeys.TASK_KEY_KEY, Long.toString(Id.Task.parse(taskId).getSequence()));
    }
  }

  /**
   * Try acquiring the job lock and return whether the lock is successfully locked.
   *
   * @param properties the job properties
   */
  private boolean tryLockJob(Properties properties) {
    try {
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, Boolean.TRUE.toString()))) {
        this.jobLockOptional = Optional.of(getJobLock(properties, new JobLockEventListener() {
          @Override
          public void onLost() {
            executeCancellation();
          }
        }));
      }
      return !this.jobLockOptional.isPresent() || this.jobLockOptional.get().tryLock();
    } catch (JobLockException ioe) {
      LOG.error(String.format("Failed to acquire job lock for job %s: %s", this.jobContext.getJobId(), ioe), ioe);
      return false;
    }
  }

  /**
   * Unlock a completed or failed job.
   */
  private void unlockJob() {
    if (this.jobLockOptional.isPresent()) {
      try {
        // Unlock so the next run of the same job can proceed
        this.jobLockOptional.get().unlock();
      } catch (JobLockException ioe) {
        LOG.error(String.format("Failed to unlock for job %s: %s", this.jobContext.getJobId(), ioe), ioe);
      } finally {
        try {
          this.jobLockOptional.get().close();
        } catch (IOException e) {
          LOG.error(String.format("Failed to close job lock for job %s: %s", this.jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e), e);
        } finally {
          this.jobLockOptional = Optional.absent();
        }
      }
    }
  }

  /**
   * Combines the specified {@link JobListener} with the {@link #mandatoryJobListeners} for this job. Uses
   * {@link JobListeners#parallelJobListener(List)} to create a {@link CloseableJobListener} that will execute all
   * the {@link JobListener}s in parallel.
   */
  private CloseableJobListener getParallelCombinedJobListener(JobState jobState, JobListener jobListener) {
    List<JobListener> jobListeners = Lists.newArrayList(this.mandatoryJobListeners);
    jobListeners.add(jobListener);

    Set<String> jobListenerClassNames = jobState.getPropAsSet(ConfigurationKeys.JOB_LISTENERS_KEY, StringUtils.EMPTY);
    for (String jobListenerClassName : jobListenerClassNames) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends JobListener> jobListenerClass =
            (Class<? extends JobListener>) Class.forName(jobListenerClassName);
        jobListeners.add(jobListenerClass.newInstance());
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        LOG.warn(String.format("JobListener could not be created due to %s", jobListenerClassName), e);
      }
    }

    return JobListeners.parallelJobListener(jobListeners);
  }

  /**
   * Takes a {@link List} of {@link Tag}s and returns a new {@link List} with the original {@link Tag}s as well as any
   * additional {@link Tag}s returned by {@link ClusterNameTags#getClusterNameTags()}.
   *
   * @see ClusterNameTags
   */
  private static List<Tag<?>> addClusterNameTags(List<? extends Tag<?>> tags) {
    return ImmutableList.<Tag<?>>builder().addAll(tags).addAll(Tag.fromMap(ClusterNameTags.getClusterNameTags()))
        .build();
  }

  /**
   * Build the {@link EventSubmitter} for this class.
   */
  private EventSubmitter buildEventSubmitter(List<? extends Tag<?>> tags) {
    return new EventSubmitter.Builder(this.runtimeMetricContext, "gobblin.runtime")
        .addMetadata(Tag.toMap(Tag.tagValuesToString(tags))).build();
  }

  /**
   * Cleanup the left-over staging data possibly from the previous run of the job that may have failed
   * and not cleaned up its staging data.
   *
   * Property {@link ConfigurationKeys#CLEANUP_STAGING_DATA_PER_TASK} controls whether to cleanup
   * staging data per task, or to cleanup entire job's staging data at once.
   *
   * Staging data will not be cleaned if the job has unfinished {@link CommitSequence}s.
   */
  private void cleanLeftoverStagingData(WorkUnitStream workUnits, JobState jobState)
      throws JobException {
    if (jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_BY_INITIALIZER, false)) {
      //Clean up will be done by initializer.
      return;
    }

    try {
      if (!canCleanStagingData(jobState)) {
        LOG.error("Job " + jobState.getJobName() + " has unfinished commit sequences. Will not clean up staging data.");
        return;
      }
    } catch (IOException e) {
      throw new JobException("Failed to check unfinished commit sequences", e);
    }

    try {
      if (this.jobContext.shouldCleanupStagingDataPerTask()) {
        if (workUnits.isSafeToMaterialize()) {
          Closer closer = Closer.create();
          Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
          try {
            for (WorkUnit workUnit : JobLauncherUtils.flattenWorkUnits(workUnits.getMaterializedWorkUnitCollection())) {
              JobLauncherUtils.cleanTaskStagingData(new WorkUnitState(workUnit, jobState), LOG, closer, parallelRunners);
            }
          } catch (Throwable t) {
            throw closer.rethrow(t);
          } finally {
            closer.close();
          }
        } else {
          throw new RuntimeException("Work unit streams do not support cleaning staging data per task.");
        }
      } else {
        if (jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_OLD_JOBS_DATA, ConfigurationKeys.DEFAULT_CLEANUP_OLD_JOBS_DATA)) {
          JobLauncherUtils.cleanUpOldJobData(jobState, LOG, jobContext.getStagingDirProvided(), jobContext.getOutputDirProvided());
        }
        JobLauncherUtils.cleanJobStagingData(jobState, LOG);
      }
    } catch (Throwable t) {
      // Catch Throwable instead of just IOException to make sure failure of this won't affect the current run
      LOG.error("Failed to clean leftover staging data", t);
    }
  }

  private static String getJobIdPrefix(String jobId) {
    return jobId.substring(0, jobId.lastIndexOf(Id.Job.SEPARATOR) + 1);
  }
  /**
   * Cleanup the job's task staging data. This is not doing anything in case job succeeds
   * and data is successfully committed because the staging data has already been moved
   * to the job output directory. But in case the job fails and data is not committed,
   * we want the staging data to be cleaned up.
   *
   * Property {@link ConfigurationKeys#CLEANUP_STAGING_DATA_PER_TASK} controls whether to cleanup
   * staging data per task, or to cleanup entire job's staging data at once.
   *
   * Staging data will not be cleaned if the job has unfinished {@link CommitSequence}s.
   */
  private void cleanupStagingData(JobState jobState)
      throws JobException {
    if (jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_BY_INITIALIZER, false)) {
      //Clean up will be done by initializer.
      return;
    }

    try {
      if (!canCleanStagingData(jobState)) {
        LOG.error("Job " + jobState.getJobName() + " has unfinished commit sequences. Will not clean up staging data.");
        return;
      }
    } catch (IOException e) {
      throw new JobException("Failed to check unfinished commit sequences", e);
    }

    if (this.jobContext.shouldCleanupStagingDataPerTask()) {
      cleanupStagingDataPerTask(jobState);
    } else {
      cleanupStagingDataForEntireJob(jobState);
    }
  }

  public boolean isEarlyStopped() {
    return this.jobContext.getSource().isEarlyStopped();
  }

  protected IssueRepository getIssueRepository() {
    return troubleshooter.getIssueRepository();
  }

  /**
   * Staging data cannot be cleaned if exactly once semantics is used, and the job has unfinished
   * commit sequences.
   */
  private boolean canCleanStagingData(JobState jobState)
      throws IOException {
    return this.jobContext.getSemantics() != DeliverySemantics.EXACTLY_ONCE || !this.jobContext.getCommitSequenceStore()
        .get().exists(jobState.getJobName());
  }

  private static void cleanupStagingDataPerTask(JobState jobState) {
    Closer closer = Closer.create();
    Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
    try {
      for (TaskState taskState : jobState.getTaskStates()) {
        try {
          JobLauncherUtils.cleanTaskStagingData(taskState, LOG, closer, parallelRunners);
        } catch (IOException e) {
          LOG.error(String.format("Failed to clean staging data for task %s: %s", taskState.getTaskId(), e), e);
        }
      }
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Failed to clean staging data", e);
      }
    }
  }

  private static void cleanupStagingDataForEntireJob(JobState jobState) {
    try {
      JobLauncherUtils.cleanJobStagingData(jobState, LOG);
    } catch (IOException e) {
      LOG.error("Failed to clean staging data for job " + jobState.getJobId(), e);
    }
  }

  private void notifyListeners(JobContext jobContext, JobListener jobListener, String timerEventName,
      JobListenerAction action)
      throws JobException {
    TimingEvent timer = this.eventSubmitter.getTimingEvent(timerEventName);
    try (CloseableJobListener parallelJobListener = getParallelCombinedJobListener(this.jobContext.getJobState(),
        jobListener)) {
      action.apply(parallelJobListener, jobContext);
    } catch (Exception e) {
      throw new JobException("Failed to execute all JobListeners", e);
    } finally {
      LOG.info("Submitting {}", timerEventName);
      timer.stop(this.multiEventMetadataGenerator.getMetadata(this.jobContext,
          EventName.getEnumFromEventId(timerEventName)));
    }
  }

  private interface JobListenerAction {
    void apply(JobListener jobListener, JobContext jobContext)
        throws Exception;
  }
}
