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

package org.apache.gobblin.compaction.verify;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.executors.ScalingThreadPoolExecutor;


/**
 * A class for verifying data completeness of a {@link Dataset}.
 *
 * To verify data completeness, one should extend {@link AbstractRunner} and implement {@link AbstractRunner#call()}
 * which returns a {@link Results} object. The (fully qualified) name of the class that extends {@link AbstractRunner}
 * should be associated with property {@link ConfigurationKeys#COMPACTION_DATA_COMPLETENESS_VERIFICATION_CLASS}.
 *
 * @author Ziyang Liu
 */
public class DataCompletenessVerifier implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessVerifier.class);

  private static final String COMPACTION_COMPLETENESS_VERIFICATION_PREFIX = "compaction.completeness.verification.";

  /**
   * Configuration properties related to data completeness verification.
   */
  private static final String COMPACTION_COMPLETENESS_VERIFICATION_CLASS =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "class";
  private static final String COMPACTION_COMPLETENESS_VERIFICATION_THREAD_POOL_SIZE =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "thread.pool.size";
  private static final int DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_THREAD_POOL_SIZE = 20;

  private final State props;
  private final int threadPoolSize;
  private final ListeningExecutorService exeSvc;
  private final Class<? extends AbstractRunner> runnerClass;

  /**
   * The given {@link State} object must specify property
   * {@link ConfigurationKeys#COMPACTION_DATA_COMPLETENESS_VERIFICATION_CLASS}, and may optionally specify
   * {@link ConfigurationKeys#COMPACTION_DATA_COMPLETENESS_VERIFICATION_THREAD_POOL_SIZE}.
   */
  public DataCompletenessVerifier(State props) {
    this.props = props;
    this.threadPoolSize = getDataCompletenessVerificationThreadPoolSize();
    this.exeSvc = getExecutorService();
    this.runnerClass = getRunnerClass();
  }

  private ListeningExecutorService getExecutorService() {
    return ExecutorsUtils.loggingDecorator(
        ScalingThreadPoolExecutor.newScalingThreadPool(0, this.threadPoolSize, TimeUnit.SECONDS.toMillis(10)));
  }

  private int getDataCompletenessVerificationThreadPoolSize() {
    return this.props.getPropAsInt(COMPACTION_COMPLETENESS_VERIFICATION_THREAD_POOL_SIZE,
        DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_THREAD_POOL_SIZE);
  }

  @SuppressWarnings("unchecked")
  private Class<? extends AbstractRunner> getRunnerClass() {
    Preconditions.checkArgument(this.props.contains(COMPACTION_COMPLETENESS_VERIFICATION_CLASS),
        "Missing required property " + COMPACTION_COMPLETENESS_VERIFICATION_CLASS);
    try {
      return (Class<? extends AbstractRunner>) Class
          .forName(this.props.getProp(COMPACTION_COMPLETENESS_VERIFICATION_CLASS));
    } catch (Throwable t) {
      LOG.error("Failed to get data completeness verification class", t);
      throw Throwables.propagate(t);
    }
  }

  /**
   * Verify data completeness for a set of {@link Dataset}s.
   *
   * @param datasets {@link Dataset}s to be verified.
   * @return A {@link ListenableFuture<{@link Results}>} object that contains the result of the verification.
   * Callers can add listeners or callbacks to it.
   */
  public ListenableFuture<Results> verify(Iterable<Dataset> datasets) {
    return this.exeSvc.submit(getRunner(datasets));
  }

  private AbstractRunner getRunner(Iterable<Dataset> datasets) {
    try {
      return this.runnerClass.getDeclaredConstructor(Iterable.class, State.class).newInstance(datasets, this.props);
    } catch (Throwable t) {
      LOG.error("Failed to instantiate data completeness verification class", t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void close() throws IOException {
    ExecutorsUtils.shutdownExecutorService(this.exeSvc, Optional.of(LOG));
  }

  public void closeNow() {
    ExecutorsUtils.shutdownExecutorService(this.exeSvc, Optional.of(LOG), 0, TimeUnit.NANOSECONDS);
  }

  /**
   * Results of data completeness verification for a set of datasets.
   */
  public static class Results implements Iterable<Results.Result> {

    private final Iterable<Result> results;

    public Results(Iterable<Result> results) {
      this.results = results;
    }

    @Override
    public Iterator<Results.Result> iterator() {
      return this.results.iterator();
    }

    public static class Result {

      public enum Status {
        PASSED,
        FAILED;
      }

      private final Dataset dataset;
      private final Status status;
      /**
       * Data used to compute this result. A verification context is used to communicate to the caller how this {@link #status()}
       * for data completeness was derived.
       */
      private final Map<String, Object> verificationContext;

      public Result(Dataset dataset, Status status) {
        this.dataset = dataset;
        this.status = status;
        this.verificationContext = ImmutableMap.of();
      }

      public Result(Dataset dataset, Status status, Map<String, Object> verificationContext) {
        this.dataset = dataset;
        this.status = status;
        this.verificationContext = verificationContext;
      }

      public Dataset dataset() {
        return this.dataset;
      }

      public Status status() {
        return this.status;
      }

      public Map<String, Object> verificationContext() {
        return this.verificationContext;
      }
    }
  }

  /**
   * Runner class for data completeness verification. Subclasses should implement {@link AbstractRunner#call()}
   * which should contain the logic of data completeness verification and returns a {@link Results} object.
   */
  public static abstract class AbstractRunner implements Callable<Results> {
    protected final Iterable<Dataset> datasets;
    protected final State props;

    public AbstractRunner(Iterable<Dataset> datasets, State props) {
      this.datasets = datasets;
      this.props = props;
    }
  }

}
