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
package gobblin.runtime.job_catalog;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.api.MutableJobCatalog;


/**
 * Simple implementation of a Gobblin job catalog that stores all JobSpecs in memory. No persistence
 * is provided.
 */
public class InMemoryJobCatalog extends MutableJobCatalogBase {
  protected final Map<URI, JobSpec> jobSpecs = new HashMap<>();

  public InMemoryJobCatalog() {
    this(Optional.<Logger>absent());
  }

  public InMemoryJobCatalog(Optional<Logger> log) {
    this(log, Optional.<MetricContext>absent(), true);
  }

  public InMemoryJobCatalog(GobblinInstanceEnvironment env) {
    this(Optional.of(env.getLog()), Optional.of(env.getMetricContext()),
         env.isInstrumentationEnabled());
  }

  public InMemoryJobCatalog(Optional<Logger> log, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) {
    super(log, parentMetricContext, instrumentationEnabled);
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized Collection<JobSpec> getJobs() {
    return new ArrayList<>(this.jobSpecs.values());
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized JobSpec getJobSpec(URI uri)
      throws JobSpecNotFoundException {
    if (this.jobSpecs.containsKey(uri)) {
      return this.jobSpecs.get(uri);
    } else {
      throw new JobSpecNotFoundException(uri);
    }
  }

  @Override
  protected JobSpec doPut(JobSpec jobSpec) {
    return this.jobSpecs.put(jobSpec.getUri(), jobSpec);
  }

  @Override
  protected JobSpec doRemove(URI uri) {
    return this.jobSpecs.remove(uri);
  }
}
