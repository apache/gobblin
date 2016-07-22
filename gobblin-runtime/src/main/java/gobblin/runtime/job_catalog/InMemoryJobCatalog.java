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
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.runtime.job_catalog.JobCatalogListenersList.AddJobCallback;

/**
 * Simple implementation of a Gobblin job catalog that stores all JobSpecs in memory. No persistence
 * is provided.
 */
public class InMemoryJobCatalog implements MutableJobCatalog {
  protected final JobCatalogListenersList listeners;
  protected final Logger log;
  protected final Map<URI, JobSpec> jobSpecs = new HashMap<>();

  public InMemoryJobCatalog() {
    this(Optional.<Logger>absent());
  }

  public InMemoryJobCatalog(Optional<Logger> log) {
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.listeners = new JobCatalogListenersList(log);
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized Collection<JobSpec> getJobs() {
    return new ArrayList<>(this.jobSpecs.values());
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized JobSpec getJobSpec(URI uri) {
    if (this.jobSpecs.containsKey(uri)) {
      return this.jobSpecs.get(uri);
    }
    else {
      throw new JobSpecNotFoundException(uri);
    }
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized void addListener(JobCatalogListener jobListener) {
    Preconditions.checkNotNull(jobListener);

    this.listeners.addListener(jobListener);
    for (Map.Entry<URI, JobSpec> jobSpecEntry: this.jobSpecs.entrySet()) {
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
    }
    else {
      this.listeners.onUpdateJob(oldSpec, jobSpec);
    }
  }

  @Override
  public synchronized void remove(URI uri) {
    Preconditions.checkNotNull(uri);
    JobSpec jobSpec = this.jobSpecs.remove(uri);
    if (null != jobSpec) {
      this.listeners.onDeleteJob(jobSpec);
    }
  }

}
