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
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.api.client.util.Lists;
import com.google.common.io.Closer;

import io.temporal.failure.ApplicationFailure;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.converter.initializer.ConverterInitializerFactory;
import org.apache.gobblin.destination.DestinationDatasetHandlerService;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooterFactory;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.WorkUnitStreamSource;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.temporal.ddm.activity.GenerateWorkUnits;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.writer.initializer.WriterInitializerFactory;


@Slf4j
public class GenerateWorkUnitsImpl implements GenerateWorkUnits {

  @Override
  public int generateWorkUnits(Properties jobProps, EventSubmitterContext eventSubmitterContext) {
    // TODO: decide whether to acquire a job lock (as MR did)!
    // TODO: provide for job cancellation (unless handling at the temporal-level of parent workflows)!
    JobState jobState = new JobState(jobProps);
    log.info("Created jobState: {}", jobState.toJsonString(true));

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
      FileSystem fs = JobStateUtils.openFileSystem(jobState);
      fs.mkdirs(workDirRoot);

      List<WorkUnit> workUnits = generateWorkUnitsForJobState(jobState, eventSubmitterContext, closer);

      JobStateUtils.writeWorkUnits(workUnits, workDirRoot, jobState, fs);
      JobStateUtils.writeJobState(jobState, workDirRoot, fs);

      return jobState.getTaskCount();
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
    }
  }

  protected static List<WorkUnit> generateWorkUnitsForJobState(JobState jobState, EventSubmitterContext eventSubmitterContext, Closer closer)
      throws ReflectiveOperationException {
    Source<?, ?> source = JobStateUtils.createSource(jobState);
    WorkUnitStream workUnitStream = source instanceof WorkUnitStreamSource
        ? ((WorkUnitStreamSource) source).getWorkunitStream(jobState)
        : new BasicWorkUnitStream.Builder(source.getWorkunits(jobState)).build();

    // TODO: report (timer) metrics for workunits creation
    if (workUnitStream == null || workUnitStream.getWorkUnits() == null) { // indicates a problem getting the WUs
      String errMsg = "Failure in getting work units for job " + jobState.getJobId();
      log.error(errMsg);
      // TODO: decide whether a non-retryable failure is too severe... (in most circumstances, it's likely what we want)
      throw ApplicationFailure.newNonRetryableFailure(errMsg, "Failure: Source.getWorkUnits()");
    }

    if (!workUnitStream.getWorkUnits().hasNext()) { // no work unit to run: entirely normal result (not a failure)
      log.warn("No work units created for job " + jobState.getJobId());
      return Lists.newArrayList();
    }

    // TODO: count total bytes for progress tracking!

    boolean canCleanUp = canCleanStagingData(jobState);
    DestinationDatasetHandlerService datasetHandlerService = closer.register(
        new DestinationDatasetHandlerService(jobState, canCleanUp, eventSubmitterContext.create()));
    WorkUnitStream handledWorkUnitStream = datasetHandlerService.executeHandlers(workUnitStream);

    // initialize writer and converter(s)
    // TODO: determine whether registration here is effective, or the lifecycle of this activity is too brief (as is likely!)
    closer.register(WriterInitializerFactory.newInstace(jobState, handledWorkUnitStream)).initialize();
    closer.register(ConverterInitializerFactory.newInstance(jobState, handledWorkUnitStream)).initialize();

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

    return AbstractJobLauncher.materializeWorkUnitList(trackedWorkUnitStream);
  }

  protected static boolean canCleanStagingData(JobState jobState) {
    if (DeliverySemantics.EXACTLY_ONCE.equals(DeliverySemantics.parse(jobState))) {
      String errMsg = "DeliverySemantics.EXACTLY_ONCE NOT currently supported; job " + jobState.getJobId();
      log.error(errMsg);
      throw ApplicationFailure.newNonRetryableFailure(errMsg, "Unsupported: DeliverySemantics.EXACTLY_ONCE");
    }
    return true;
  }
}
