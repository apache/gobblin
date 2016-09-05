/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.job_catalog;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.State;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobCatalogListener.AddJobCallback;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.api.MutableJobCatalog;


/**
 * Simple implementation of a Gobblin job catalog that stores all JobSpecs in memory. No persistence
 * is provided.
 */
public class InMemoryJobCatalog implements MutableJobCatalog {
  protected final JobCatalogListenersList listeners;
  protected final Logger log;
  protected final Map<URI, JobSpec> jobSpecs = new HashMap<>();
  protected final MetricContext metricContext;
  protected final StandardMetrics metrics;

  public InMemoryJobCatalog() {
    this(Optional.<Logger>absent());
  }

  public InMemoryJobCatalog(Optional<Logger> log) {
    this(log, Optional.<MetricContext>absent(), true);
  }

  public InMemoryJobCatalog(Optional<Logger> log, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) {
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.listeners = new JobCatalogListenersList(log);
    if (instrumentationEnabled) {
      MetricContext realParentCtx =
          parentMetricContext.or(Instrumented.getMetricContext(new State(), getClass()));
      this.metricContext = realParentCtx.childBuilder(JobCatalog.class.getSimpleName()).build();
      this.metrics = new StandardMetrics(this);
    }
    else {
      this.metricContext = null;
      this.metrics = null;
    }
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

  /**{@inheritDoc}*/
  @Override
  public synchronized void addListener(JobCatalogListener jobListener) {
    Preconditions.checkNotNull(jobListener);

    this.listeners.addListener(jobListener);
    for (Map.Entry<URI, JobSpec> jobSpecEntry : this.jobSpecs.entrySet()) {
      AddJobCallback addJobCallback = new AddJobCallback(jobSpecEntry.getValue());
      this.listeners.callbackOneListener(addJobCallback, jobListener);
    }
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized void removeListener(JobCatalogListener jobListener) {
    this.listeners.removeListener(jobListener);
  }

  @Override
  public synchronized void put(JobSpec jobSpec) {
    Preconditions.checkNotNull(jobSpec);
    JobSpec oldSpec = this.jobSpecs.put(jobSpec.getUri(), jobSpec);
    if (null == oldSpec) {
      this.listeners.onAddJob(jobSpec);
    } else {
      this.listeners.onUpdateJob(jobSpec);
    }
  }

  @Override
  public synchronized void remove(URI uri) {
    Preconditions.checkNotNull(uri);
    JobSpec jobSpec = this.jobSpecs.remove(uri);
    if (null != jobSpec) {
      this.listeners.onDeleteJob(jobSpec.getUri(), jobSpec.getVersion());
    }
  }

  @Override
  public void registerWeakJobCatalogListener(JobCatalogListener jobListener) {
    this.listeners.registerWeakJobCatalogListener(jobListener);
  }

  @Override public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override public boolean isInstrumentationEnabled() {
    return null != this.metricContext;
  }

  @Override public List<Tag<?>> generateTags(State state) {
    return Collections.emptyList();
  }

  @Override public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Override public StandardMetrics getMetrics() {
    return this.metrics;
  }
}
