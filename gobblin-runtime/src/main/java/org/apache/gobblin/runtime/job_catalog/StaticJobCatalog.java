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

package org.apache.gobblin.runtime.job_catalog;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.JobCatalogListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecNotFoundException;


/**
 * A {@link org.apache.gobblin.runtime.api.JobCatalog} with a static collection of {@link JobSpec}s defined at construction time.
 */
public class StaticJobCatalog extends JobCatalogBase {

  private final Map<URI, JobSpec> jobs;

  public StaticJobCatalog(Collection<JobSpec> jobSpecs) {
    this.jobs = parseJobs(jobSpecs);
  }

  public StaticJobCatalog(Optional<Logger> log, Collection<JobSpec> jobSpecs) {
    super(log);
    this.jobs = parseJobs(jobSpecs);
  }

  public StaticJobCatalog(GobblinInstanceEnvironment env, Collection<JobSpec> jobSpecs) {
    super(env);
    this.jobs = parseJobs(jobSpecs);
  }

  public StaticJobCatalog(Optional<Logger> log, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled, Collection<JobSpec> jobSpecs) {
    super(log, parentMetricContext, instrumentationEnabled);
    this.jobs = parseJobs(jobSpecs);
  }

  private Map<URI, JobSpec> parseJobs(Collection<JobSpec> jobSpecs) {
    ImmutableMap.Builder<URI, JobSpec> mapBuilder = ImmutableMap.builder();
    for (JobSpec jobSpec : jobSpecs) {
      mapBuilder.put(jobSpec.getUri(), jobSpec);
    }
    return mapBuilder.build();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "UR_UNINIT_READ_CALLED_FROM_SUPER_CONSTRUCTOR",
      justification = "Uninitialized variable has been checked.")
  @Override
  public void addListener(JobCatalogListener jobListener) {
    if (this.jobs == null) {
      return;
    }
    for (Map.Entry<URI, JobSpec> entry : this.jobs.entrySet()) {
      jobListener.onAddJob(entry.getValue());
    }
  }

  @Override
  public Collection<JobSpec> getJobs() {
    return this.jobs.values();
  }

  @Override
  public void removeListener(JobCatalogListener jobListener) {
    // NOOP
  }

  @Override
  public JobSpec getJobSpec(URI uri)
      throws JobSpecNotFoundException {
    return this.jobs.get(uri);
  }
}
