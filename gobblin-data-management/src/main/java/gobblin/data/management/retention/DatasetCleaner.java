/*
* Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use
* this file except in compliance with the License. You may obtain a copy of the
* License at  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied.
*/

package gobblin.data.management.retention;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;
import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.util.ExecutorsUtils;
import gobblin.util.RateControlledFileSystem;
import gobblin.util.executors.ScalingThreadPoolExecutor;


/**
 * Finds existing versions of datasets and cleans old or deprecated versions.
 */
public class DatasetCleaner implements Instrumentable, Closeable {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String DATASET_PROFILE_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "dataset.profile.class";
  public static final String MAX_CONCURRENT_DATASETS_CLEANED = CONFIGURATION_KEY_PREFIX
      + "max.concurrent.datasets.cleaned";
  public static final String DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT = CONFIGURATION_KEY_PREFIX
      + "hdfs.calls.per.second.limit";

  public static final String DEFAULT_MAX_CONCURRENT_DATASETS_CLEANED = "1000";

  private static Logger LOG = LoggerFactory.getLogger(DatasetCleaner.class);

  private final DatasetFinder datasetFinder;
  private final ListeningExecutorService service;
  private final Closer closer;
  private final boolean isMetricEnabled;
  private MetricContext metricContext;
  private Optional<Meter> datasetsCleanSuccessMeter = Optional.absent();
  private Optional<Meter> datasetsCleanFailureMeter = Optional.absent();
  private Optional<CountDownLatch> finishCleanSignal;

  public DatasetCleaner(FileSystem fs, Properties props) throws IOException {

    Preconditions.checkArgument(props.containsKey(DATASET_PROFILE_CLASS_KEY));
    this.closer = Closer.create();
    try {
      FileSystem optionalRateControlledFs = fs;
      if (props.contains(DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT)) {
        optionalRateControlledFs =
            this.closer.register(new RateControlledFileSystem(fs, Long.parseLong(props
                .getProperty(DATASET_CLEAN_HDFS_CALLS_PER_SECOND_LIMIT))));
        ((RateControlledFileSystem) optionalRateControlledFs).startRateControl();
      }
      Class<?> datasetFinderClass = Class.forName(props.getProperty(DATASET_PROFILE_CLASS_KEY));
      this.datasetFinder =
          (DatasetFinder) datasetFinderClass.getConstructor(FileSystem.class, Properties.class).newInstance(
              optionalRateControlledFs, props);
    } catch (ClassNotFoundException exception) {
      throw new IOException(exception);
    } catch (NoSuchMethodException exception) {
      throw new IOException(exception);
    } catch (InstantiationException exception) {
      throw new IOException(exception);
    } catch (IllegalAccessException exception) {
      throw new IOException(exception);
    } catch (InvocationTargetException exception) {
      throw new IOException(exception);
    } catch (NumberFormatException exception) {
      throw new IOException(exception);
    } catch (ExecutionException exception) {
      throw new IOException(exception);
    }
    ExecutorService executor =
        ScalingThreadPoolExecutor.newScalingThreadPool(0, Integer
            .parseInt(props.getProperty(MAX_CONCURRENT_DATASETS_CLEANED, DEFAULT_MAX_CONCURRENT_DATASETS_CLEANED)), 100,
            ExecutorsUtils.newThreadFactory(Optional.of(LOG),
            Optional.of("Dataset-cleaner-pool-%d")));
    this.service = MoreExecutors.listeningDecorator(executor);

    // TODO -- Remove the dependency on gobblin-core after new Gobblin Metrics does not depend on gobblin-core.
    this.metricContext = this.closer.register(Instrumented.getMetricContext(new State(props), DatasetCleaner.class));
    this.isMetricEnabled = GobblinMetrics.isEnabled(props);
  }

  /**
   * Perform the cleanup of old / deprecated dataset versions.
   * @throws IOException
   */
  public void clean() throws IOException {
    List<Dataset> dataSets = this.datasetFinder.findDatasets();
    finishCleanSignal = Optional.of(new CountDownLatch(dataSets.size()));
    for (final Dataset dataset : dataSets) {
      ListenableFuture<Void> future = this.service.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          dataset.clean();
          return null;
        }
      });
      Futures.addCallback(future, new FutureCallback<Void>() {
        @Override
        public void onFailure(Throwable throwable) {
          finishCleanSignal.get().countDown();
          LOG.warn("Exception caught when cleaning " + dataset.datasetRoot() + ".", throwable);
          Instrumented.markMeter(datasetsCleanFailureMeter);
        }

        @Override
        public void onSuccess(Void arg0) {
          finishCleanSignal.get().countDown();
          LOG.info("Successfully cleaned: " + dataset.datasetRoot());
          Instrumented.markMeter(datasetsCleanSuccessMeter);
        }

      });
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.finishCleanSignal.isPresent()) {
        finishCleanSignal.get().await();
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
    this.metricContext =
        this.closer.register(Instrumented.newContextFromReferenceContext(this.metricContext, tags,
            Optional.<String> absent()));
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

  public static class DatasetCleanerMetrics {
    public static final String DATASETS_CLEAN_SUCCESS = "gobblin.retention.datasets.clean.success";
    public static final String DATASETS_CLEAN_FAILURE = "gobblin.retention.datasets.clean.failure";
  }
}
