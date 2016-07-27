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
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecNotFoundException;

/**
 * A JobCatalog decorator that caches all JobSpecs in memory.
 *
 */
public class CachingJobCatalog implements JobCatalog {
  protected final JobCatalog _fallback;
  protected final InMemoryJobCatalog _cache;
  protected final Logger _log;

  public CachingJobCatalog(JobCatalog fallback, Optional<Logger> log) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _fallback = fallback;
    _cache = new InMemoryJobCatalog(log);
    _fallback.addListener(new FallbackCatalogListener());
  }

  /** {@inheritDoc} */
  @Override
  public Collection<JobSpec> getJobs() {
    return _cache.getJobs();
  }

  /** {@inheritDoc} */
  @Override
  public JobSpec getJobSpec(URI uri) throws JobSpecNotFoundException {
    try {
      return _cache.getJobSpec(uri);
    }
    catch (RuntimeException e) {
      return _fallback.getJobSpec(uri);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void addListener(JobCatalogListener jobListener) {
    _cache.addListener(jobListener);
  }

  /** {@inheritDoc} */
  @Override
  public void removeListener(JobCatalogListener jobListener) {
    _cache.removeListener(jobListener);
  }

  /** Refreshes the cache if the underlying fallback catalog changes. */
  private class FallbackCatalogListener implements JobCatalogListener {

    @Override
    public void onAddJob(JobSpec addedJob) {
      _cache.put(addedJob);
    }

    @Override
    public void onDeleteJob(JobSpec deletedJob) {
      _cache.remove(deletedJob.getUri());
    }

    @Override
    public void onUpdateJob(JobSpec originalJob, JobSpec updatedJob) {
      _cache.put(updatedJob);
    }

  }

}
