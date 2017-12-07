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
package org.apache.gobblin.runtime.api;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.GobblinMetricsKeys;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareHistogram;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A catalog of all the {@link JobSpec}s a Gobblin instance is currently aware of.
 */
@Alpha
public interface JobCatalog extends JobCatalogListenersContainer, Instrumentable, StandardMetricsBridge {
  /** Returns an immutable {@link Collection} of {@link JobSpec}s that are known to the catalog. */
  Collection<JobSpec> getJobs();

  /** Metrics for the job catalog; null if
   * ({@link #isInstrumentationEnabled()}) is false. */
  JobCatalog.StandardMetrics getMetrics();

  default StandardMetricsBridge.StandardMetrics getStandardMetrics() {
    return getMetrics();
  }

  /**
   * Get a {@link JobSpec} by uri.
   * @throws JobSpecNotFoundException if no such JobSpec exists
   **/
  JobSpec getJobSpec(URI uri) throws JobSpecNotFoundException;

  @Slf4j
  public static class StandardMetrics extends StandardMetricsBridge.StandardMetrics implements JobCatalogListener {
    public static final String NUM_ACTIVE_JOBS_NAME = "numActiveJobs";
    public static final String NUM_ADDED_JOBS = "numAddedJobs";
    public static final String NUM_DELETED_JOBS = "numDeletedJobs";
    public static final String NUM_UPDATED_JOBS = "numUpdatedJobs";
    public static final String TIME_FOR_JOB_CATALOG_GET = "timeForJobCatalogGet";
    public static final String HISTOGRAM_FOR_JOB_ADD = "histogramForJobAdd";
    public static final String HISTOGRAM_FOR_JOB_UPDATE = "histogramForJobUpdate";
    public static final String HISTOGRAM_FOR_JOB_DELETE = "histogramForJobDelete";
    public static final String TRACKING_EVENT_NAME = "JobCatalogEvent";
    public static final String JOB_ADDED_OPERATION_TYPE = "JobAdded";
    public static final String JOB_DELETED_OPERATION_TYPE = "JobDeleted";
    public static final String JOB_UPDATED_OPERATION_TYPE = "JobUpdated";

    @Getter private final ContextAwareGauge<Integer> numActiveJobs;
    @Getter private final ContextAwareCounter numAddedJobs;
    @Getter private final ContextAwareCounter numDeletedJobs;
    @Getter private final ContextAwareCounter numUpdatedJobs;
    @Getter private final ContextAwareTimer timeForJobCatalogGet;
    @Getter private final ContextAwareHistogram histogramForJobAdd;
    @Getter private final ContextAwareHistogram histogramForJobUpdate;
    @Getter private final ContextAwareHistogram histogramForJobDelete;

    public StandardMetrics(final JobCatalog jobCatalog) {
      MetricContext context = jobCatalog.getMetricContext();
      this.timeForJobCatalogGet = context.contextAwareTimer(TIME_FOR_JOB_CATALOG_GET, 1, TimeUnit.MINUTES);
      this.numAddedJobs = context.contextAwareCounter(NUM_ADDED_JOBS);
      this.numDeletedJobs = context.contextAwareCounter(NUM_DELETED_JOBS);
      this.numUpdatedJobs = context.contextAwareCounter(NUM_UPDATED_JOBS);
      this.numActiveJobs = context.newContextAwareGauge(NUM_ACTIVE_JOBS_NAME, ()->{
          long startTime = System.currentTimeMillis();
          int size = jobCatalog.getJobs().size();
          updateGetJobTime(startTime);
          return size;
      });
      this.histogramForJobAdd = jobCatalog.getMetricContext().contextAwareHistogram(HISTOGRAM_FOR_JOB_ADD, 1, TimeUnit.MINUTES);
      this.histogramForJobUpdate = jobCatalog.getMetricContext().contextAwareHistogram(HISTOGRAM_FOR_JOB_UPDATE, 1, TimeUnit.MINUTES);
      this.histogramForJobDelete = jobCatalog.getMetricContext().contextAwareHistogram(HISTOGRAM_FOR_JOB_DELETE, 1, TimeUnit.MINUTES);
    }

    public void updateGetJobTime(long startTime) {
      log.info("updateGetJobTime...");
      Instrumented.updateTimer(Optional.of(this.timeForJobCatalogGet), System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
    }

    @Override public void onAddJob(JobSpec addedJob) {
      this.numAddedJobs.inc();
      this.histogramForJobAdd.update(1);
      submitTrackingEvent(addedJob, JOB_ADDED_OPERATION_TYPE);
    }

    private void submitTrackingEvent(JobSpec job, String operType) {
      submitTrackingEvent(job.getUri(), job.getVersion(), operType);
    }

    private void submitTrackingEvent(URI jobSpecURI, String jobSpecVersion, String operType) {
      GobblinTrackingEvent e = GobblinTrackingEvent.newBuilder()
          .setName(TRACKING_EVENT_NAME)
          .setNamespace(JobCatalog.class.getName())
          .setMetadata(ImmutableMap.<String, String>builder()
              .put(GobblinMetricsKeys.OPERATION_TYPE_META, operType)
              .put(GobblinMetricsKeys.JOB_SPEC_URI_META, jobSpecURI.toString())
              .put(GobblinMetricsKeys.JOB_SPEC_VERSION_META, jobSpecVersion)
              .build())
          .build();
      this.numAddedJobs.getContext().submitEvent(e);
    }

    @Override
    public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
      this.numDeletedJobs.inc();
      this.histogramForJobDelete.update(1);
      submitTrackingEvent(deletedJobURI, deletedJobVersion, JOB_DELETED_OPERATION_TYPE);
    }

    @Override
    public void onUpdateJob(JobSpec updatedJob) {
      this.numUpdatedJobs.inc();
      this.histogramForJobUpdate.update(1);
      submitTrackingEvent(updatedJob, JOB_UPDATED_OPERATION_TYPE);
    }

    @Override
    public Collection<ContextAwareGauge<?>> getGauges() {
      return Collections.singleton(this.numActiveJobs);
    }

    @Override
    public Collection<ContextAwareCounter> getCounters() {
      return ImmutableList.of(numAddedJobs, numDeletedJobs, numUpdatedJobs);
    }

    @Override
    public Collection<ContextAwareTimer> getTimers() {
      return ImmutableList.of(timeForJobCatalogGet);
    }

    @Override
    public Collection<ContextAwareHistogram> getHistograms() {
      return ImmutableList.of(histogramForJobAdd, histogramForJobDelete, histogramForJobUpdate);
    }
  }
}
