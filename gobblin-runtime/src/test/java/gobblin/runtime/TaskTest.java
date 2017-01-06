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

package gobblin.runtime;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.fork.IdentityForkOperator;
import gobblin.publisher.TaskPublisher;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;


/**
 * Integration tests for {@link Task}.
 */
@Test
public class TaskTest {

  /**
   * Check if a {@link WorkUnitState.WorkingState} of a {@link Task} is set properly after a {@link Task} fails once,
   * but then is successful the next time.
   */
  @Test
  public void testRetryTask() throws Exception {
    // Create a TaskState
    TaskState taskState = new TaskState(new WorkUnitState(WorkUnit.create(
        new Extract(Extract.TableType.SNAPSHOT_ONLY, this.getClass().getName(), this.getClass().getSimpleName()))));
    taskState.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(false));
    taskState.setTaskId("testRetryTaskId");

    // Create a mock TaskContext
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getExtractor()).thenReturn(new FailOnceExtractor());
    when(mockTaskContext.getForkOperator()).thenReturn(new IdentityForkOperator());
    when(mockTaskContext.getTaskState()).thenReturn(taskState);
    when(mockTaskContext.getTaskLevelPolicyChecker(any(TaskState.class), anyInt()))
        .thenReturn(mock(TaskLevelPolicyChecker.class));

    // Create a mock TaskPublisher
    TaskPublisher mockTaskPublisher = mock(TaskPublisher.class);
    when(mockTaskPublisher.canPublish()).thenReturn(TaskPublisher.PublisherState.SUCCESS);
    when(mockTaskContext.getTaskPublisher(any(TaskState.class), any(TaskLevelPolicyCheckResults.class)))
        .thenReturn(mockTaskPublisher);

    // Create a mock TaskStateTracker
    TaskStateTracker mockTaskStateTracker = mock(TaskStateTracker.class);

    // Create a TaskExecutor - a real TaskExecutor must be created so a Fork is run in a separate thread
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());

    // Create the Task
    Task realTask = new Task(mockTaskContext, mockTaskStateTracker, taskExecutor, Optional.<CountDownLatch> absent());
    Task task = spy(realTask);
    doNothing().when(task).submitTaskCommittedEvent();

    // The first run of the Task should fail
    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.FAILED);

    // The second run of the Task should succeed
    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
  }

  /**
   * An implementation of {@link Extractor} that throws an {@link IOException} during the invocation of
   * {@link #readRecord(Object)}.
   */
  private static class FailOnceExtractor implements Extractor<Object, Object> {

    private static final AtomicBoolean HAS_FAILED = new AtomicBoolean();

    @Override
    public Object getSchema() throws IOException {
      return null;
    }

    @Override
    public Object readRecord(@Deprecated Object reuse) throws DataRecordException, IOException {
      if (!HAS_FAILED.get()) {
        HAS_FAILED.set(true);
        throw new IOException("Injected failure");
      }
      return null;
    }

    @Override
    public long getExpectedRecordCount() {
      return -1;
    }

    @Override
    public long getHighWatermark() {
      return -1;
    }

    @Override
    public void close() throws IOException {
      // Do nothing
    }
  }
}
