/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;

import gobblin.rest.TaskExecutionInfo;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;


/**
 * Unit tests for {@link TaskState}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime"})
public class TaskStateTest {

  private TaskState taskState;
  private long startTime;

  @BeforeClass
  public void setUp() {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, "Job-1");
    workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, "Task-1");
    this.taskState = new TaskState(workUnitState);
  }

  @Test
  public void testSetAndGet() {
    this.taskState.setId("Task-1");
    this.taskState.setHighWaterMark(2000);
    this.startTime = System.currentTimeMillis();
    this.taskState.setStartTime(this.startTime);
    this.taskState.setEndTime(this.startTime + 1000);
    this.taskState.setTaskDuration(1000);
    this.taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    this.taskState.setProp("foo", "bar");

    Assert.assertEquals(this.taskState.getJobId(), "Job-1");
    Assert.assertEquals(this.taskState.getTaskId(), "Task-1");
    Assert.assertEquals(this.taskState.getId(), "Task-1");
    Assert.assertEquals(this.taskState.getHighWaterMark(), 2000);
    Assert.assertEquals(this.taskState.getStartTime(), this.startTime);
    Assert.assertEquals(this.taskState.getEndTime(), this.startTime + 1000);
    Assert.assertEquals(this.taskState.getTaskDuration(), 1000);
    Assert.assertEquals(this.taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
    Assert.assertEquals(this.taskState.getProp("foo"), "bar");
  }

  @Test(dependsOnMethods = {"testSetAndGet"})
  public void testSerDe()
      throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
      DataOutputStream dos = closer.register(new DataOutputStream(baos));
      this.taskState.write(dos);

      ByteArrayInputStream bais = closer.register((new ByteArrayInputStream(baos.toByteArray())));
      DataInputStream dis = closer.register((new DataInputStream(bais)));
      TaskState newTaskState = new TaskState();
      newTaskState.readFields(dis);

      Assert.assertEquals(newTaskState.getJobId(), "Job-1");
      Assert.assertEquals(newTaskState.getTaskId(), "Task-1");
      Assert.assertEquals(this.taskState.getHighWaterMark(), 2000);
      Assert.assertEquals(newTaskState.getStartTime(), this.startTime);
      Assert.assertEquals(newTaskState.getEndTime(), this.startTime + 1000);
      Assert.assertEquals(newTaskState.getTaskDuration(), 1000);
      Assert.assertEquals(newTaskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(newTaskState.getProp("foo"), "bar");
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Test(dependsOnMethods = {"testSetAndGet"})
  public void testToTaskExecutionInfo() {
    TaskExecutionInfo taskExecutionInfo = this.taskState.toTaskExecutionInfo();
    Assert.assertEquals(taskExecutionInfo.getJobId(), "Job-1");
    Assert.assertEquals(taskExecutionInfo.getTaskId(), "Task-1");
    Assert.assertEquals(taskExecutionInfo.getHighWatermark().longValue(), 2000L);
    Assert.assertEquals(taskExecutionInfo.getStartTime().longValue(), this.startTime);
    Assert.assertEquals(taskExecutionInfo.getEndTime().longValue(), this.startTime + 1000);
    Assert.assertEquals(taskExecutionInfo.getDuration().longValue(), 1000L);
    Assert.assertEquals(taskExecutionInfo.getState().name(), WorkUnitState.WorkingState.COMMITTED.name());
    Assert.assertEquals(taskExecutionInfo.getTaskProperties().get("foo"), "bar");
  }
}
