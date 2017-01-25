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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;


/**
 * An abstract base for {@link JobCatalog}s implementing boilerplate methods (metrics, listeners, etc.)
 */
public abstract class JobCatalogBase extends AbstractIdleService implements JobCatalog {

  protected final JobCatalogListenersList listeners;
  protected final Logger log;
  protected final MetricContext metricContext;
  protected final StandardMetrics metrics;

  public JobCatalogBase() {
    this(Optional.<Logger>absent());
  }

  public JobCatalogBase(Optional<Logger> log) {
    this(log, Optional.<MetricContext>absent(), true);
  }

  public JobCatalogBase(GobblinInstanceEnvironment env) {
    this(Optional.of(env.getLog()), Optional.of(env.getMetricContext()),
        env.isInstrumentationEnabled());
  }

  public JobCatalogBase(Optional<Logger> log, Optional<MetricContext> parentMetricContext,
      boolean instrumentationEnabled) {
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.listeners = new JobCatalogListenersList(log);
    if (instrumentationEnabled) {
      MetricContext realParentCtx =
          parentMetricContext.or(Instrumented.getMetricContext(new gobblin.configuration.State(), getClass()));
      this.metricContext = realParentCtx.childBuilder(JobCatalog.class.getSimpleName()).build();
      this.metrics = new StandardMetrics(this);
    }
    else {
      this.metricContext = null;
      this.metrics = null;
    }
  }

  @Override
  protected void startUp() throws IOException {
    notifyAllListeners();
  }

  @Override
  protected void shutDown() throws IOException {
    this.listeners.close();
  }

  protected void notifyAllListeners() {
    for (JobSpec jobSpec : getJobs()) {
      this.listeners.onAddJob(jobSpec);
    }
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized void addListener(JobCatalogListener jobListener) {
    Preconditions.checkNotNull(jobListener);
    this.listeners.addListener(jobListener);

    if (state() == State.RUNNING) {
      for (JobSpec jobSpec : getJobs()) {
        JobCatalogListener.AddJobCallback addJobCallback = new JobCatalogListener.AddJobCallback(jobSpec);
        this.listeners.callbackOneListener(addJobCallback, jobListener);
      }
    }
  }

  /**{@inheritDoc}*/
  @Override
  public synchronized void removeListener(JobCatalogListener jobListener) {
    this.listeners.removeListener(jobListener);
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

  @Override public List<Tag<?>> generateTags(gobblin.configuration.State state) {
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
