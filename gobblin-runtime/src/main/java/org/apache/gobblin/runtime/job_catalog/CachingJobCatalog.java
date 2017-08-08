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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.JobCatalog;
import org.apache.gobblin.runtime.api.JobCatalogListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecNotFoundException;

/**
 * A JobCatalog decorator that caches all JobSpecs in memory.
 *
 */
public class CachingJobCatalog extends AbstractIdleService implements JobCatalog {
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

  @Override
  protected void startUp() {
    _cache.startAsync();
    try {
      _cache.awaitRunning(2, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      throw new RuntimeException("Failed to start " + CachingJobCatalog.class.getName(), te);
    }
  }

  @Override
  protected void shutDown() {
    _cache.stopAsync();
    try {
      _cache.awaitTerminated(2, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      throw new RuntimeException("Failed to stop " + CachingJobCatalog.class.getName(), te);
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
    public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
      _cache.remove(deletedJobURI);
    }

    @Override
    public void onUpdateJob(JobSpec updatedJob) {
      _cache.put(updatedJob);
    }

  }

  @Override
  public void registerWeakJobCatalogListener(JobCatalogListener jobListener) {
    _cache.registerWeakJobCatalogListener(jobListener);
  }

  @Override public MetricContext getMetricContext() {
    return _fallback.getMetricContext();
  }

  @Override public boolean isInstrumentationEnabled() {
    return _fallback.isInstrumentationEnabled();
  }

  @Override public List<Tag<?>> generateTags(org.apache.gobblin.configuration.State state) {
    return _fallback.generateTags(state);
  }

  @Override public void switchMetricContext(List<Tag<?>> tags) {
    _fallback.switchMetricContext(tags);
  }

  @Override public void switchMetricContext(MetricContext context) {
    _fallback.switchMetricContext(context);
  }

  @Override public StandardMetrics getMetrics() {
    return _fallback.getMetrics();
  }

}
