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

package org.apache.gobblin.temporal.ddm.activity.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.google.api.client.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.tdunning.math.stats.TDigest;
import io.temporal.failure.ApplicationFailure;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.initializer.ConverterInitializer;
import org.apache.gobblin.converter.initializer.ConverterInitializerFactory;
import org.apache.gobblin.initializer.Initializer;
import org.apache.gobblin.destination.DestinationDatasetHandlerService;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooterFactory;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.WorkUnitStreamSource;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.temporal.ddm.activity.GenerateWorkUnits;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.GenerateWorkUnitsResult;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.temporal.workflows.metrics.EventTimer;
import org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.writer.initializer.WriterInitializer;
import org.apache.gobblin.writer.initializer.WriterInitializerFactory;


@Slf4j
public class GenerateWorkUnitsImpl implements GenerateWorkUnits {

  /** [Internal, implementation class] Size sketch/digest of a collection of {@link MultiWorkUnit}s */
  @Data
  @VisibleForTesting
  protected static class WorkUnitsSizeDigest {
    private final long totalSize;
    /** a top-level work unit has no parent - a root */
    private final TDigest topLevelWorkUnitsSizeDigest;
    /** a constituent work unit has no children - a leaf */
    private final TDigest constituentWorkUnitsSizeDigest;

    public WorkUnitsSizeSummary asSizeSummary(int numQuantiles) {
      Preconditions.checkArgument(numQuantiles > 0, "numQuantiles must be > 0");
      final double quantilesWidth = 1.0 / numQuantiles;

      List<Double> topLevelQuantileValues = getQuantiles(topLevelWorkUnitsSizeDigest, numQuantiles);
      List<Double> constituentQuantileValues = getQuantiles(constituentWorkUnitsSizeDigest, numQuantiles);
      return new WorkUnitsSizeSummary(
          totalSize,
          topLevelWorkUnitsSizeDigest.size(), constituentWorkUnitsSizeDigest.size(),
          numQuantiles, quantilesWidth,
          topLevelQuantileValues, constituentQuantileValues);
    }

    private static List<Double> getQuantiles(TDigest digest, int numQuantiles) {
      List<Double> quantileMinSizes = Lists.newArrayList();
      for (int i = 1; i <= numQuantiles; i++) {
        double currQuantileMinSize = digest.quantile((i * 1.0) / numQuantiles);
        if (Double.isNaN(currQuantileMinSize)) {
          currQuantileMinSize = 0.0;
        }
        quantileMinSizes.add(currQuantileMinSize);
      }
      return quantileMinSizes;
    }
  }


  /** [Internal, impl class] Intermediate result of generated work units with insightful analysis extracted by pre-processing them */
  @Data
  private static class WorkUnitsWithInsights {
    private final List<WorkUnit> workUnits;
    private final Set<String> pathsToCleanUp;
    private final Optional<Initializer.AfterInitializeMemento> optWriterMemento;
    private final Optional<Initializer.AfterInitializeMemento> optConverterMemento;
  }


  @Override
  public GenerateWorkUnitsResult generateWorkUnits(Properties jobProps, EventSubmitterContext eventSubmitterContext) {
    ActivityExecutionContext activityExecutionContext = Activity.getExecutionContext();
    ScheduledExecutorService heartBeatExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(com.google.common.base.Optional.of(log),
            com.google.common.base.Optional.of("GenerateWorkUnitsActivityHeartBeatExecutor")));
    // TODO: decide whether to acquire a job lock (as MR did)!
    // TODO: provide for job cancellation (unless handling at the temporal-level of parent workflows)!
    JobState jobState = new JobState(jobProps);
    log.info("Created jobState: {}", jobState.toJsonString(true));

    int heartBeatInterval = JobStateUtils.getHeartBeatInterval(jobState);
    heartBeatExecutor.scheduleAtFixedRate(() -> activityExecutionContext.heartbeat("Running GenerateWorkUnits"),
        heartBeatInterval, heartBeatInterval, TimeUnit.MINUTES);

    Path workDirRoot = JobStateUtils.getWorkDirRoot(jobState);
    log.info("Using work dir root path for job '{}' - '{}'", jobState.getJobId(), workDirRoot);

    // TODO: determine whether these are actually necessary to do (as MR/AbstractJobLauncher did)!
    // SharedResourcesBroker<GobblinScopeTypes> jobBroker = JobStateUtils.getSharedResourcesBroker(jobState);
    // jobState.setBroker(jobBroker);
    // jobState.setWorkUnitAndDatasetStateFunctional(new CombinedWorkUnitAndDatasetStateGenerator(this.datasetStateStore, this.jobName));

    AutomaticTroubleshooter troubleshooter = AutomaticTroubleshooterFactory.createForJob(jobProps);
    troubleshooter.start();
    try (Closer closer = Closer.create()) {
      // before embarking on (potentially expensive) WU creation, first pre-check that the FS is available
      FileSystem fs = closer.register(JobStateUtils.openFileSystem(jobState));
      fs.mkdirs(workDirRoot);

      WorkUnitsWithInsights genWUsInsights = generateWorkUnitsForJobStateAndCollectCleanupPaths(jobState, eventSubmitterContext, closer);
      List<WorkUnit> workUnits = genWUsInsights.getWorkUnits();

      int numSizeSummaryQuantiles = getConfiguredNumSizeSummaryQuantiles(jobState);
      WorkUnitsSizeSummary wuSizeSummary = digestWorkUnitsSize(workUnits).asSizeSummary(numSizeSummaryQuantiles);
      log.info("Discovered WorkUnits: {}", wuSizeSummary);
      // IMPORTANT: send prior to `writeWorkUnits`, so the volume of work discovered (and bin packed) gets durably measured.  even if serialization were to
      // exceed available memory and this activity execution were to fail, a subsequent re-attempt would know the amount of work, to guide re-config/attempt
      createWorkPreparedSizeDistillationTimer(wuSizeSummary, eventSubmitterContext).stop();

      // add any (serialized) mementos before serializing `jobState`, for later `recall` during `CommitActivityImpl`
      genWUsInsights.optWriterMemento.ifPresent(memento ->
          jobState.setProp(ConfigurationKeys.WRITER_INITIALIZER_SERIALIZED_MEMENTO_KEY,
              Initializer.AfterInitializeMemento.serialize(memento))
      );
      genWUsInsights.optConverterMemento.ifPresent(memento ->
          jobState.setProp(ConfigurationKeys.CONVERTER_INITIALIZERS_SERIALIZED_MEMENTOS_KEY,
              Initializer.AfterInitializeMemento.serialize(memento))
      );
      JobStateUtils.writeWorkUnits(workUnits, workDirRoot, jobState, fs);
      JobStateUtils.writeJobState(jobState, workDirRoot, fs); // ATTENTION: the writing of `JobState` after all WUs signifies WU gen+serialization now complete

      String sourceClassName = JobStateUtils.getSourceClassName(jobState);
      return new GenerateWorkUnitsResult(jobState.getTaskCount(), sourceClassName, wuSizeSummary, genWUsInsights.getPathsToCleanUp());
    } catch (ReflectiveOperationException roe) {
      String errMsg = "Unable to construct a source for generating workunits for job " + jobState.getJobId();
      log.error(errMsg, roe);
      throw ApplicationFailure.newNonRetryableFailureWithCause(errMsg, "Failure: new Source()", roe);
    } catch (IOException ioe) {
      String errMsg = "Failed to generate workunits for job " + jobState.getJobId();
      log.error(errMsg, ioe);
      throw ApplicationFailure.newFailureWithCause(errMsg, "Failure: generating/writing workunits", ioe);
    } finally {
      EventSubmitter eventSubmitter = eventSubmitterContext.create();
      Help.finalizeTroubleshooting(troubleshooter, eventSubmitter, log, jobState.getJobId());
      ExecutorsUtils.shutdownExecutorService(heartBeatExecutor, com.google.common.base.Optional.of(log));
    }
  }

  protected WorkUnitsWithInsights generateWorkUnitsForJobStateAndCollectCleanupPaths(JobState jobState, EventSubmitterContext eventSubmitterContext, Closer closer)
      throws ReflectiveOperationException {
    // report (timer) metrics for "Work Discovery", *planning only* - NOT including WU prep, like serialization, `DestinationDatasetHandlerService`ing, etc.
    // IMPORTANT: for accurate timing, SEPARATELY emit `.createWorkPreparationTimer`, to record time prior to measuring the WU size required for that one
    TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.WithinActivityFactory(eventSubmitterContext);
    EventTimer workDiscoveryTimer = timerFactory.createWorkDiscoveryTimer();
    Source<?, ?> source = JobStateUtils.createSource(jobState);
    WorkUnitStream workUnitStream = source instanceof WorkUnitStreamSource
        ? ((WorkUnitStreamSource) source).getWorkunitStream(jobState)
        : new BasicWorkUnitStream.Builder(source.getWorkunits(jobState)).build();

    if (workUnitStream == null || workUnitStream.getWorkUnits() == null) { // indicates a problem getting the WUs
      String errMsg = "Failure in getting work units for job " + jobState.getJobId();
      log.error(errMsg);
      // TODO: decide whether a non-retryable failure is too severe... (some sources may merit retry)
      throw ApplicationFailure.newNonRetryableFailure(errMsg, "Failure: Source.getWorkUnits()");
    }
    workDiscoveryTimer.stop();

    if (!workUnitStream.getWorkUnits().hasNext()) { // no work unit to run: entirely normal result (not a failure)
      log.warn("No work units created for job " + jobState.getJobId());
      return new WorkUnitsWithInsights(Lists.newArrayList(), new HashSet<>(), Optional.empty(), Optional.empty());
    }

    boolean canCleanUpTempDirs = false; // unlike `AbstractJobLauncher` running the job end-to-end, this is Work Discovery only, so WAY TOO SOON for cleanup
    DestinationDatasetHandlerService datasetHandlerService = closer.register(
        new DestinationDatasetHandlerService(jobState, canCleanUpTempDirs, eventSubmitterContext.create()));
    WorkUnitStream handledWorkUnitStream = datasetHandlerService.executeHandlers(workUnitStream);
    Set<String> pathsToCleanUp = new HashSet<>(calculateWorkDirsToCleanup(handledWorkUnitStream));

    // initialize writer and converter(s), but DO NOT `.close()` them here; rather `.commemorate()` for later `.recall()` during Commit
    WriterInitializer writerInitializer = WriterInitializerFactory.newInstace(jobState, handledWorkUnitStream);
    writerInitializer.initialize();
    ConverterInitializer converterInitializer = ConverterInitializerFactory.newInstance(jobState, handledWorkUnitStream);
    converterInitializer.initialize();

    // update jobState before it gets serialized
    long startTime = System.currentTimeMillis();
    jobState.setStartTime(startTime);
    jobState.setState(JobState.RunningState.RUNNING);

    log.info("Starting job " + jobState.getJobId());
    // TODO: report (timer) metrics for workunits preparation
    WorkUnitStream preparedWorkUnitStream = AbstractJobLauncher.prepareWorkUnits(handledWorkUnitStream, jobState);

    // TODO: gobblinJobMetricsReporter.reportWorkUnitCountMetrics(this.jobContext.getJobState().getPropAsInt(NUM_WORKUNITS), jobState);

    // dump the work unit if tracking logs are enabled (post any materialization for counting)
    WorkUnitStream trackedWorkUnitStream = AbstractJobLauncher.addWorkUnitTrackingPerConfig(preparedWorkUnitStream, jobState, log);

    return new WorkUnitsWithInsights(
        AbstractJobLauncher.materializeWorkUnitList(trackedWorkUnitStream),
        pathsToCleanUp,
        writerInitializer.commemorate(),
        converterInitializer.commemorate());
  }

  protected static Set<String> calculateWorkDirsToCleanup(WorkUnitStream workUnitStream) {
    Set<String> workDirPaths = new HashSet<>();
    // Validate every workunit if they have the temp dir props since some workunits may be commit steps
    Iterator<WorkUnit> workUnitIterator = workUnitStream.getWorkUnits();
    while (workUnitIterator.hasNext()) {
      WorkUnit workUnit = workUnitIterator.next();
      if (workUnit.isMultiWorkUnit()) {
        List<WorkUnit> workUnitList = ((MultiWorkUnit) workUnit).getWorkUnits();
        for (WorkUnit wu : workUnitList) {
          // WARNING/TODO: NOT resilient to nested multi-workunits... should it be?
          workDirPaths.addAll(collectTaskStagingAndOutputDirsFromWorkUnit(wu));
        }
      } else {
        workDirPaths.addAll(collectTaskStagingAndOutputDirsFromWorkUnit(workUnit));
      }
    }
    return workDirPaths;
  }

  private static Set<String> collectTaskStagingAndOutputDirsFromWorkUnit(WorkUnit workUnit) {
    Set<String> resourcesToCleanUp = new HashSet<>();
    if (workUnit.contains(ConfigurationKeys.WRITER_STAGING_DIR)) {
      resourcesToCleanUp.add(workUnit.getProp(ConfigurationKeys.WRITER_STAGING_DIR));
    }
    if (workUnit.contains(ConfigurationKeys.WRITER_OUTPUT_DIR)) {
      resourcesToCleanUp.add(workUnit.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));
    }
    if (workUnit.getPropAsBoolean(ConfigurationKeys.CLEAN_ERR_DIR, ConfigurationKeys.DEFAULT_CLEAN_ERR_DIR)
        && workUnit.contains(ConfigurationKeys.ROW_LEVEL_ERR_FILE)) {
      resourcesToCleanUp.add(workUnit.getProp(ConfigurationKeys.ROW_LEVEL_ERR_FILE));
    }
    return resourcesToCleanUp;
  }

  /** @return the {@link WorkUnitsSizeDigest} for `workUnits` */
  protected static WorkUnitsSizeDigest digestWorkUnitsSize(List<WorkUnit> workUnits) {
    AtomicLong totalSize = new AtomicLong(0L);
    TDigest topLevelWorkUnitsDigest = TDigest.createDigest(100);
    TDigest constituentWorkUnitsDigest = TDigest.createDigest(100);

    Iterator<WorkUnit> workUnitIterator = workUnits.iterator();
    while (workUnitIterator.hasNext()) {
      WorkUnit workUnit = workUnitIterator.next();
      if (workUnit.isMultiWorkUnit()) {
        List<WorkUnit> subWorkUnitsList = ((MultiWorkUnit) workUnit).getWorkUnits();
        AtomicLong mwuAggSize = new AtomicLong(0L);
        // WARNING/TODO: NOT resilient to nested multi-workunits... should it be?
        subWorkUnitsList.stream().mapToLong(wu -> wu.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE, 0)).forEach(wuSize -> {
          constituentWorkUnitsDigest.add(wuSize);
          mwuAggSize.addAndGet(wuSize);
        });
        totalSize.addAndGet(mwuAggSize.get());
        topLevelWorkUnitsDigest.add(mwuAggSize.get());
      } else {
        long wuSize = workUnit.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE, 0);
        totalSize.addAndGet(wuSize);
        constituentWorkUnitsDigest.add(wuSize);
        topLevelWorkUnitsDigest.add(wuSize);
      }
    }

    // TODO - decide whether helpful/necessary to `.compress()`
    topLevelWorkUnitsDigest.compress();
    constituentWorkUnitsDigest.compress();
    return new WorkUnitsSizeDigest(totalSize.get(), topLevelWorkUnitsDigest, constituentWorkUnitsDigest);
  }

  protected static EventTimer createWorkPreparedSizeDistillationTimer(
      WorkUnitsSizeSummary wuSizeSummary, EventSubmitterContext eventSubmitterContext) {
    // Inspired by a pair of log messages produced within `CopySource::getWorkUnits`:
    //   1. Statistics for ConcurrentBoundedPriorityIterable: {ResourcePool: {softBound: [ ... ], hardBound: [ ...]},totalResourcesUsed: [ ... ], \
    //       maxRequirementPerDimension: [entities: 231943.0, bytesCopied: 1.22419622769628E14], ... }
    //   2. org.apache.gobblin.data.management.copy.CopySource - Bin packed work units. Initial work units: 27252, packed work units: 13175, \
    //       max weight per bin: 500000000, max work units per bin: 100.
    // rather than merely logging, durably emit this info, to inform re-config for any potential re-attempt (should WU serialization OOM)
    TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.WithinActivityFactory(eventSubmitterContext);
    return timerFactory.createWorkPreparationTimer()
        .withMetadataAsJson(TimingEvent.WORKUNITS_GENERATED_SUMMARY, wuSizeSummary.distill());
  }

  public static int getConfiguredNumSizeSummaryQuantiles(State state) {
    return state.getPropAsInt(GenerateWorkUnits.NUM_WORK_UNITS_SIZE_SUMMARY_QUANTILES, GenerateWorkUnits.DEFAULT_NUM_WORK_UNITS_SIZE_SUMMARY_QUANTILES);
  }
}
