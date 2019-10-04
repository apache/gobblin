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

package org.apache.gobblin.data.management.retention;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.retention.dataset.finder.TimeBasedStateStoreDatasetFinder;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metastore.StateStoreDataset;
import org.apache.gobblin.metastore.StateStoreDatasetFinder;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.AzkabanTags;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.RateControlledFileSystem;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.util.executors.ScalingThreadPoolExecutor;


/**
 * Finds existing versions of datasets and cleans old or deprecated versions.
 */
public class StateCleaner implements Instrumentable, Closeable {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String MAX_CONCURRENT_DATASETS_CLEANED =
      CONFIGURATION_KEY_PREFIX + "max.concurrent.datasets.cleaned";
  public static final String DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT =
      CONFIGURATION_KEY_PREFIX + "hdfs.calls.per.second.limit";

  public static final String DEFAULT_MAX_CONCURRENT_DATASETS_CLEANED = "100";

  private static Logger LOG = LoggerFactory.getLogger(StateCleaner.class);

  private final StateStoreDatasetFinder datasetFinder;
  private final ListeningExecutorService service;
  private final Closer closer;
  private final boolean isMetricEnabled;
  private MetricContext metricContext;
  private final EventSubmitter eventSubmitter;
  private Optional<Meter> datasetsCleanSuccessMeter = Optional.absent();
  private Optional<Meter> datasetsCleanFailureMeter = Optional.absent();
  private Optional<CountDownLatch> finishCleanSignal;
  private final List<Throwable> throwables;

  public StateCleaner(FileSystem fs, Properties props) throws IOException {

    State state = new State(props);
    FileSystem targetFs =
        props.containsKey(ConfigurationKeys.WRITER_FILE_SYSTEM_URI) ? WriterUtils.getWriterFs(state) : fs;
    this.closer = Closer.create();
    // TODO -- Remove the dependency on gobblin-core after new Gobblin Metrics does not depend on gobblin-core.
    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
    this.metricContext =
        this.closer.register(Instrumented.getMetricContext(new State(props), StateCleaner.class, tags));
    this.isMetricEnabled = GobblinMetrics.isEnabled(props);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, RetentionEvents.NAMESPACE).build();
    try {
      FileSystem optionalRateControlledFs;
      if (props.contains(DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT)) {
        optionalRateControlledFs = this.closer.register(new RateControlledFileSystem(targetFs,
            Long.parseLong(props.getProperty(DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT))));
        ((RateControlledFileSystem) optionalRateControlledFs).startRateControl();
      }

      this.datasetFinder = new TimeBasedStateStoreDatasetFinder(props);
    } catch (NumberFormatException | ExecutionException exception) {
      throw new IOException(exception);
    }
    ExecutorService executor = ScalingThreadPoolExecutor.newScalingThreadPool(0,
        Integer.parseInt(props.getProperty(MAX_CONCURRENT_DATASETS_CLEANED, DEFAULT_MAX_CONCURRENT_DATASETS_CLEANED)),
        100, ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of("Dataset-cleaner-pool-%d")));
    this.service = ExecutorsUtils.loggingDecorator(executor);
    this.throwables = Lists.newArrayList();
  }

  /**
   * Perform the cleanup of old / deprecated states versions.
   * @throws IOException
   */
  public void clean() throws IOException {
    List<StateStoreDataset> states = this.datasetFinder.findStates();

    this.finishCleanSignal = Optional.of(new CountDownLatch(states.size()));
    for (final StateStoreDataset state : states) {
      ListenableFuture<Void> future = this.service.submit(() -> {
        if (state instanceof CleanableDataset) {
          ((CleanableDataset) state).clean();
        }
        return null;
      });
      Futures.addCallback(future, new FutureCallback<Void>() {
        @Override
        public void onFailure(Throwable throwable) {
          StateCleaner.this.finishCleanSignal.get().countDown();
          LOG.warn("Exception caught when cleaning " + state.datasetURN() + ".", throwable);
          StateCleaner.this.throwables.add(throwable);
          Instrumented.markMeter(StateCleaner.this.datasetsCleanFailureMeter);
          StateCleaner.this.eventSubmitter.submit(RetentionEvents.CleanFailed.EVENT_NAME,
              ImmutableMap.of(RetentionEvents.CleanFailed.FAILURE_CONTEXT_METADATA_KEY,
                  ExceptionUtils.getFullStackTrace(throwable), RetentionEvents.DATASET_URN_METADATA_KEY, state.datasetURN()));
        }

        @Override
        public void onSuccess(Void arg0) {
          StateCleaner.this.finishCleanSignal.get().countDown();
          LOG.info("Successfully cleaned: " + state.datasetURN());
          Instrumented.markMeter(StateCleaner.this.datasetsCleanSuccessMeter);
        }
      });
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.finishCleanSignal.isPresent()) {
        this.finishCleanSignal.get().await();
      }
      if (!this.throwables.isEmpty()) {
        for (Throwable t : this.throwables) {
          LOG.error("Failed clean due to ", t);
        }
        throw new RuntimeException("Clean failed for one or more datasets");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Not all datasets finish cleanning", e);
    } finally {
      ExecutorsUtils.shutdownExecutorService(this.service, Optional.of(LOG));
      this.closer.close();
    }
  }

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
    this.metricContext = this.closer.register(
        Instrumented.newContextFromReferenceContext(this.metricContext, tags, Optional.<String>absent()));
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
  protected void regenerateMetrics() {
    if (isInstrumentationEnabled()) {
      this.datasetsCleanFailureMeter =
          Optional.of(this.metricContext.meter(DatasetCleanerMetrics.DATASETS_CLEAN_FAILURE));
      this.datasetsCleanSuccessMeter =
          Optional.of(this.metricContext.meter(DatasetCleanerMetrics.DATASETS_CLEAN_SUCCESS));
    }
  }

  private static class DatasetCleanerMetrics {
    private static final String DATASETS_CLEAN_SUCCESS = "gobblin.retention.datasets.clean.success";
    private static final String DATASETS_CLEAN_FAILURE = "gobblin.retention.datasets.clean.failure";
  }
}
