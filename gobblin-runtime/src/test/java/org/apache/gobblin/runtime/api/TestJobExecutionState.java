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
package org.apache.gobblin.runtime.api;

import static org.apache.gobblin.configuration.ConfigurationKeys.JOB_NAME_KEY;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState.RunningState;
import org.apache.gobblin.runtime.std.JobExecutionUpdatable;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ExecutorsUtils;

/**
 * Unit tests for {@link JobExecutionState}
 */
public class TestJobExecutionState {

  @Test public void testStateTransitionsSuccess() throws TimeoutException, InterruptedException {
    final Logger log = LoggerFactory.getLogger(getClass().getSimpleName() + ".testStateTransitionsSuccess");
    JobSpec js1 = JobSpec.builder("gobblin:/testStateTransitionsSuccess/job1")
        .withConfig(ConfigFactory.empty()
            .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("myJob")))
        .build();
    JobExecution je1 = JobExecutionUpdatable.createFromJobSpec(js1);

    final JobExecutionStateListener listener = mock(JobExecutionStateListener.class);

    final JobExecutionState jes1 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    // Current state is null
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.COMMITTED);
    assertFailedStateTransition(jes1, RunningState.SUCCESSFUL);
    assertFailedStateTransition(jes1, RunningState.FAILED);
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.CANCELLED);

    assertTransition(jes1, listener, null, RunningState.PENDING, log);

    // Current state is PENDING
    assertFailedStateTransition(jes1, RunningState.PENDING);
    assertFailedStateTransition(jes1, RunningState.COMMITTED);
    assertFailedStateTransition(jes1, RunningState.SUCCESSFUL);

    assertTransition(jes1, listener, RunningState.PENDING, RunningState.RUNNING, log);

    // Current state is RUNNING
    assertFailedStateTransition(jes1, RunningState.PENDING);
    assertFailedStateTransition(jes1, RunningState.COMMITTED);
    assertFailedStateTransition(jes1, RunningState.RUNNING);

    assertTransition(jes1, listener, RunningState.RUNNING, RunningState.SUCCESSFUL, log);

    // Current state is SUCCESSFUL
    assertFailedStateTransition(jes1, RunningState.PENDING);
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.SUCCESSFUL);

    assertTransition(jes1, listener, RunningState.SUCCESSFUL, RunningState.COMMITTED, log);

    // Current state is COMMITTED (final)
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.COMMITTED);
    assertFailedStateTransition(jes1, RunningState.SUCCESSFUL);
    assertFailedStateTransition(jes1, RunningState.FAILED);
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.CANCELLED);
  }


  @Test public void testStateTransitionsFailure() throws TimeoutException, InterruptedException {
    final Logger log = LoggerFactory.getLogger(getClass().getSimpleName() + ".testStateTransitionsFailure");
    JobSpec js1 = JobSpec.builder("gobblin:/testStateTransitionsFailure/job1")
        .withConfig(ConfigFactory.empty()
            .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("myJob")))
        .build();
    JobExecution je1 = JobExecutionUpdatable.createFromJobSpec(js1);

    final JobExecutionStateListener listener = mock(JobExecutionStateListener.class);

    final JobExecutionState jes1 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    assertTransition(jes1, listener, null, RunningState.PENDING, log);
    assertTransition(jes1, listener, RunningState.PENDING, RunningState.FAILED, log);

    // Current state is FAILED (final)
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.COMMITTED);
    assertFailedStateTransition(jes1, RunningState.SUCCESSFUL);
    assertFailedStateTransition(jes1, RunningState.FAILED);
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.CANCELLED);

    final JobExecutionState jes2 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    assertTransition(jes2, listener, null, RunningState.PENDING, log);
    assertTransition(jes2, listener, RunningState.PENDING, RunningState.RUNNING, log);
    assertTransition(jes2, listener, RunningState.RUNNING, RunningState.FAILED, log);

    final JobExecutionState je3 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    assertTransition(je3, listener, null, RunningState.PENDING, log);
    assertTransition(je3, listener, RunningState.PENDING, RunningState.RUNNING, log);
    assertTransition(je3, listener, RunningState.RUNNING, RunningState.SUCCESSFUL, log);
    assertTransition(je3, listener, RunningState.SUCCESSFUL, RunningState.FAILED, log);
  }

  @Test public void testStateTransitionsCancel() throws TimeoutException, InterruptedException {
    final Logger log = LoggerFactory.getLogger(getClass().getSimpleName() + ".testStateTransitionsCancel");
    JobSpec js1 = JobSpec.builder("gobblin:/testStateTransitionsCancel/job1")
        .withConfig(ConfigFactory.empty()
            .withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("myJob")))
        .build();
    JobExecution je1 = JobExecutionUpdatable.createFromJobSpec(js1);

    final JobExecutionStateListener listener = mock(JobExecutionStateListener.class);

    final JobExecutionState jes1 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    assertTransition(jes1, listener, null, RunningState.PENDING, log);
    assertTransition(jes1, listener, RunningState.PENDING, RunningState.CANCELLED, log);

    // Current state is CANCELLED (final)
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.COMMITTED);
    assertFailedStateTransition(jes1, RunningState.SUCCESSFUL);
    assertFailedStateTransition(jes1, RunningState.FAILED);
    assertFailedStateTransition(jes1, RunningState.RUNNING);
    assertFailedStateTransition(jes1, RunningState.CANCELLED);

    final JobExecutionState jes2 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    assertTransition(jes2, listener, null, RunningState.PENDING, log);
    assertTransition(jes2, listener, RunningState.PENDING, RunningState.RUNNING, log);
    assertTransition(jes2, listener, RunningState.RUNNING, RunningState.CANCELLED, log);

    final JobExecutionState je3 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    assertTransition(je3, listener, null, RunningState.PENDING, log);
    assertTransition(je3, listener, RunningState.PENDING, RunningState.RUNNING, log);
    assertTransition(je3, listener, RunningState.RUNNING, RunningState.SUCCESSFUL, log);
    assertTransition(je3, listener, RunningState.SUCCESSFUL, RunningState.CANCELLED, log);
  }

  private void assertFailedStateTransition(final JobExecutionState jes1, RunningState newState) {
    try {
      jes1.setRunningState(newState);
      Assert.fail("Exception expected");
    }
    catch (IllegalStateException e) {
      // OK
    }
  }

  private void assertTransition(final JobExecutionState jes1,
      final JobExecutionStateListener listener,
      final RunningState fromState,  final RunningState toState,
      final Logger log) throws TimeoutException, InterruptedException {

    jes1.setRunningState(toState);
    Assert.assertEquals(jes1.getRunningState(), toState);
    AssertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override public boolean apply(Void input) {
        try {
          verify(listener).onStatusChange(eq(jes1), eq(fromState), eq(toState));
        }
        catch (Throwable t) {
          // ignore
        }
        return true;
      }
    }, 50, "expecting state callback", log, 2.0, 10);
  }

  @Test public void testAwait() throws InterruptedException {
    final Logger log = LoggerFactory.getLogger(getClass().getSimpleName() + ".testAwait");
    Properties properties = new Properties();
    properties.setProperty(JOB_NAME_KEY, "jobname");
    JobSpec js1 = JobSpec.builder("gobblin:/testAwaitForDone/job1")
            .withConfigAsProperties(properties)
            .build();
    JobExecution je1 = JobExecutionUpdatable.createFromJobSpec(js1);

    final JobExecutionState jes1 =
        new JobExecutionState(js1, je1, Optional.<JobExecutionStateListener>absent());

    final AtomicBoolean doneDetected = new AtomicBoolean(false);
    ThreadFactory doneThreadFactory =
        ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of("doneDetectionThread"));
    Thread doneDetectionThread = doneThreadFactory.newThread(new Runnable() {
      @Override public void run() {
        try {
          jes1.awaitForDone(0);
        } catch (InterruptedException | TimeoutException e) {
          log.error("Error detected: " + e);
        }
        doneDetected.set(jes1.getRunningState().isDone());
      }
    });
    doneDetectionThread.start();


    long startTime = System.currentTimeMillis();
    try {
      jes1.awaitForState(RunningState.RUNNING, 10);
      Assert.fail("Timeout expected");
    } catch (TimeoutException e) {
      long now = System.currentTimeMillis();
      Assert.assertTrue(now - startTime >= 10, "Insufficient wait: " + (now - startTime));
    }

    jes1.switchToPending();
    jes1.switchToRunning();

    try {
      jes1.awaitForState(RunningState.RUNNING, 10);
      Assert.assertEquals(jes1.getRunningState(), RunningState.RUNNING);
    } catch (TimeoutException e) {
      Assert.fail("Timeout: ");
    }

    Assert.assertTrue(doneDetectionThread.isAlive());
    jes1.switchToFailed();

    doneDetectionThread.join(50);
    Assert.assertFalse(doneDetectionThread.isAlive());
    Assert.assertTrue(doneDetected.get());
  }

}
