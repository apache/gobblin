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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.io.Files;

import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.Id;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JobContextTest {

  @Test
  public void testNonParallelCommit()
      throws Exception {

    Properties jobProps = new Properties();

    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_id_12345");
    jobProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");

    Map<String, JobState.DatasetState> datasetStateMap = Maps.newHashMap();
    for (int i = 0; i < 2; i++) {
      datasetStateMap.put(Integer.toString(i), new JobState.DatasetState());
    }

    final BlockingQueue<ControllableCallable<Void>> callables = Queues.newLinkedBlockingQueue();

    final JobContext jobContext =
        new ControllableCommitJobContext(jobProps, log, datasetStateMap, new Predicate<String>() {
          @Override
          public boolean apply(@Nullable String input) {
            return true;
          }
        }, callables);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future future = executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          jobContext.commit();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });

    // Not parallelized, should only one commit running
    ControllableCallable<Void> callable = callables.poll(1, TimeUnit.SECONDS);
    Assert.assertNotNull(callable);
    Assert.assertNull(callables.poll(200, TimeUnit.MILLISECONDS));

    // unblock first commit, should see a second commit
    callable.unblock();
    callable = callables.poll(1, TimeUnit.SECONDS);
    Assert.assertNotNull(callable);
    Assert.assertNull(callables.poll(200, TimeUnit.MILLISECONDS));
    Assert.assertFalse(future.isDone());

    // unblock second commit, commit should complete
    callable.unblock();
    future.get(1, TimeUnit.SECONDS);
    Assert.assertEquals(jobContext.getJobState().getState(), JobState.RunningState.COMMITTED);
  }

  @Test
  public void testParallelCommit()
      throws Exception {

    Properties jobProps = new Properties();

    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_id_12345");
    jobProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");
    jobProps.setProperty(ConfigurationKeys.PARALLELIZE_DATASET_COMMIT, "true");

    Map<String, JobState.DatasetState> datasetStateMap = Maps.newHashMap();
    for (int i = 0; i < 5; i++) {
      datasetStateMap.put(Integer.toString(i), new JobState.DatasetState());
    }

    final BlockingQueue<ControllableCallable<Void>> callables = Queues.newLinkedBlockingQueue();

    final JobContext jobContext =
        new ControllableCommitJobContext(jobProps, log, datasetStateMap, new Predicate<String>() {
          @Override
          public boolean apply(@Nullable String input) {
            return true;
          }
        }, callables);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future future = executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          jobContext.commit();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });

    // Parallelized, should be able to get all 5 commits running
    Queue<ControllableCallable<Void>> drainedCallables = Lists.newLinkedList();
    Assert.assertEquals(Queues.drain(callables, drainedCallables, 5, 1, TimeUnit.SECONDS), 5);
    Assert.assertFalse(future.isDone());

    // unblock all commits
    for (ControllableCallable<Void> callable : drainedCallables) {
      callable.unblock();
    }

    // check that future is done
    future.get(1, TimeUnit.SECONDS);

    // check that no more commits were added
    Assert.assertTrue(callables.isEmpty());
    Assert.assertEquals(jobContext.getJobState().getState(), JobState.RunningState.COMMITTED);
  }

  @Test
  public void testSingleExceptionSemantics()
      throws Exception {

    Properties jobProps = new Properties();

    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_id_12345");
    jobProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");

    Map<String, JobState.DatasetState> datasetStateMap = Maps.newHashMap();
    for (int i = 0; i < 3; i++) {
      datasetStateMap.put(Integer.toString(i), new JobState.DatasetState());
    }

    final BlockingQueue<ControllableCallable<Void>> callables = Queues.newLinkedBlockingQueue();

    // There are three datasets, "0", "1", and "2", middle one will fail
    final JobContext jobContext =
        new ControllableCommitJobContext(jobProps, log, datasetStateMap, new Predicate<String>() {
          @Override
          public boolean apply(@Nullable String input) {
            return !input.equals("1");
          }
        }, callables);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future future = executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          jobContext.commit();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });

    // All three commits should be run (even though second one fails)
    callables.poll(1, TimeUnit.SECONDS).unblock();
    callables.poll(1, TimeUnit.SECONDS).unblock();
    callables.poll(1, TimeUnit.SECONDS).unblock();

    try {
      // check future is done
      future.get(1, TimeUnit.SECONDS);
      Assert.fail();
    } catch (ExecutionException ee) {
      // future should fail
    }
    // job failed
    Assert.assertEquals(jobContext.getJobState().getState(), JobState.RunningState.FAILED);
  }

  @Test
  public void testCleanUpOldJobData() throws Exception {
    String rootPath = Files.createTempDir().getAbsolutePath();
    final String JOB_PREFIX = Id.Job.PREFIX;
    final String JOB_NAME1 = "GobblinKafka";
    final String JOB_NAME2 = "GobblinBrooklin";
    final long timestamp1 = 1505774129247L;
    final long timestamp2 = 1505774129248L;
    final Joiner JOINER = Joiner.on(Id.SEPARATOR).skipNulls();
    Object[] oldJob1 = new Object[]{JOB_PREFIX, JOB_NAME1, timestamp1};
    Object[] oldJob2 = new Object[]{JOB_PREFIX, JOB_NAME2, timestamp1};
    Object[] currentJob = new Object[]{JOB_PREFIX, JOB_NAME1, timestamp2};

    Path currentJobPath = new Path(JobContext.getJobDir(rootPath, JOB_NAME1), JOINER.join(currentJob));
    Path oldJobPath1 = new Path(JobContext.getJobDir(rootPath, JOB_NAME1), JOINER.join(oldJob1));
    Path oldJobPath2 = new Path(JobContext.getJobDir(rootPath, JOB_NAME2), JOINER.join(oldJob2));
    Path stagingPath = new Path(currentJobPath, "task-staging");
    Path outputPath = new Path(currentJobPath, "task-output");

    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.mkdirs(currentJobPath);
    fs.mkdirs(oldJobPath1);
    fs.mkdirs(oldJobPath2);
    log.info("Created : {} {} {}", currentJobPath, oldJobPath1, oldJobPath2);

    gobblin.runtime.JobState jobState = new gobblin.runtime.JobState();
    jobState.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingPath.toString());
    jobState.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, outputPath.toString());

    JobContext jobContext = mock(JobContext.class);
    when(jobContext.getStagingDirProvided()).thenReturn(false);
    when(jobContext.getOutputDirProvided()).thenReturn(false);
    when(jobContext.getJobId()).thenReturn(currentJobPath.getName().toString());

    JobLauncherUtils.cleanUpOldJobData(jobState, log, jobContext.getStagingDirProvided(), jobContext.getOutputDirProvided());

    Assert.assertFalse(fs.exists(oldJobPath1));
    Assert.assertTrue(fs.exists(oldJobPath2));
    Assert.assertFalse(fs.exists(currentJobPath));
  }

  @Test
  public void testDatasetStateFailure() throws Exception {
    Properties jobProps = new Properties();

    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_id_12345");
    jobProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");

    Map<String, JobState.DatasetState> datasetStateMap = Maps.newHashMap();
    JobState.DatasetState failingDatasetState = new JobState.DatasetState("DatasetState", "DatasetState-1");
    // mark dataset state as a failing job
    failingDatasetState.incrementJobFailures();
    JobState.DatasetState failingDatasetState2 = new JobState.DatasetState("DatasetState2", "DatasetState-2");
    failingDatasetState2.incrementJobFailures();
    failingDatasetState2.incrementJobFailures();

    datasetStateMap.put("0", failingDatasetState);
    datasetStateMap.put("1", failingDatasetState2);

    final BlockingQueue<ControllableCallable<Void>> callables = Queues.newLinkedBlockingQueue();

    JobContext jobContext = new ControllableCommitJobContext(jobProps, log, datasetStateMap, new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String input) {
        return !input.equals("1");
      }
    }, callables);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future future = executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          jobContext.commit();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });
    callables.poll(1, TimeUnit.SECONDS).unblock();
    callables.poll(1, TimeUnit.SECONDS).unblock();

    // when checking the number of failures, this should detect the failing dataset state
    Assert.assertEquals(jobContext.getDatasetStateFailures(), 3);
  }

  @Test
  public void testNoDatasetStates() throws Exception {
    Properties jobProps = new Properties();

    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "job_id_12345");
    jobProps.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");

    Map<String, JobState.DatasetState> datasetStateMap = Maps.newHashMap();

    final BlockingQueue<ControllableCallable<Void>> callables = Queues.newLinkedBlockingQueue();

    JobContext jobContext = new ControllableCommitJobContext(jobProps, log, datasetStateMap, new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String input) {
        return !input.equals("1");
      }
    }, callables);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future future = executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          jobContext.commit();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });
    // when checking the number of failures, this should detect the failing dataset state
    Assert.assertEquals(jobContext.getDatasetStateFailures(), 0);
  }

  /**
   * A {@link Callable} that blocks until a different thread calls {@link #unblock()}.
   */
  private class ControllableCallable<T> implements Callable<T> {

    private final BlockingQueue<Boolean> queue;
    private final Either<T, Exception> toReturn;
    private final String name;

    public ControllableCallable(Either<T, Exception> toReturn, String name) {
      this.queue = Queues.newArrayBlockingQueue(1);
      this.queue.add(true);
      this.toReturn = toReturn;
      this.name = name;
    }

    public void unblock() {
      if (!this.queue.isEmpty()) {
        this.queue.poll();
      }
    }

    @Override
    public T call()
        throws Exception {
      this.queue.put(false);
      if (this.toReturn instanceof Either.Left) {
        return ((Either.Left<T, Exception>) this.toReturn).getLeft();
      } else {
        throw ((Either.Right<T, Exception>) this.toReturn).getRight();
      }
    }
  }

  private class ControllableCommitJobContext extends DummyJobContext {

    private final Predicate<String> successPredicate;
    private final Queue<ControllableCallable<Void>> callablesQueue;

    public ControllableCommitJobContext(Properties jobProps, Logger logger,
        Map<String, JobState.DatasetState> datasetStateMap, Predicate<String> successPredicate,
        Queue<ControllableCallable<Void>> callablesQueue)
        throws Exception {
      super(jobProps, logger, datasetStateMap);
      this.successPredicate = successPredicate;
      this.callablesQueue = callablesQueue;
    }

    @Override
    protected Callable<Void> createSafeDatasetCommit(boolean shouldCommitDataInJob, boolean isJobCancelled,
        DeliverySemantics deliverySemantics, String datasetUrn, JobState.DatasetState datasetState,
        boolean isMultithreaded, JobContext jobContext) {
      ControllableCallable<Void> callable;
      if (this.successPredicate.apply(datasetUrn)) {
        callable = new ControllableCallable<>(Either.<Void, Exception>left(null), datasetUrn);
      } else {
        callable = new ControllableCallable<>(Either.<Void, Exception>right(new RuntimeException("Fail!")), datasetUrn);
      }
      this.callablesQueue.add(callable);
      return callable;
    }
  }
}
