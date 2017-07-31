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
package org.apache.gobblin.compliance;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.AzkabanTags;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.executors.ScalingThreadPoolExecutor;


/**
 * Abstract class for the compliance jobs.
 *
 * @author adsharma
 */
public abstract class ComplianceJob implements Closeable, Instrumentable {

  protected Properties properties;
  protected Optional<CountDownLatch> finishCleanSignal;
  protected final ListeningExecutorService service;

  protected DatasetsFinder finder;
  protected final List<Throwable> throwables;
  protected final Closer closer;
  protected final boolean isMetricEnabled;
  protected MetricContext metricContext;
  protected final EventSubmitter eventSubmitter;

  public ComplianceJob(Properties properties) {
    this.properties = properties;
    ExecutorService executor = ScalingThreadPoolExecutor.newScalingThreadPool(0,
        Integer.parseInt(properties.getProperty(ComplianceConfigurationKeys.MAX_CONCURRENT_DATASETS, ComplianceConfigurationKeys.DEFAULT_MAX_CONCURRENT_DATASETS)), 100,
        ExecutorsUtils.newThreadFactory(Optional.<Logger>absent(), Optional.of("complaince-job-pool-%d")));
    this.service = MoreExecutors.listeningDecorator(executor);
    this.closer = Closer.create();
    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
    this.metricContext =
        this.closer.register(Instrumented.getMetricContext(new State(properties), ComplianceJob.class, tags));
    this.isMetricEnabled = GobblinMetrics.isEnabled(properties);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, ComplianceEvents.NAMESPACE).build();
    this.throwables = Lists.newArrayList();
  }

  public abstract void run()
      throws IOException;

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return this.isMetricEnabled;
  }

  /** Default with no additional tags */
  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    this.metricContext = this.closer
        .register(Instrumented.newContextFromReferenceContext(this.metricContext, tags, Optional.<String>absent()));
    this.regenerateMetrics();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    this.metricContext = context;
    this.regenerateMetrics();
  }

  /**
   * Generates metrics for the instrumentation of this class.
   */
  protected abstract void regenerateMetrics();
}
