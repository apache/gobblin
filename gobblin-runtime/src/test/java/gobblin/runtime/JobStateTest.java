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
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.rest.JobExecutionInfo;
import gobblin.rest.TaskExecutionInfo;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;


/**
 * Unit test for {@link JobState}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime"})
public class JobStateTest {

  private JobState jobState;
  private long startTime;

  @BeforeClass
  public void setUp() {
    this.jobState = new JobState("TestJob", "TestJob-1");
  }

  @Test
  public void testSetAndGet() {
    this.jobState.setId(this.jobState.getJobId());
    this.startTime = System.currentTimeMillis();
    this.jobState.setStartTime(this.startTime);
    this.jobState.setEndTime(this.startTime + 1000);
    this.jobState.setDuration(1000);
    this.jobState.setState(JobState.RunningState.COMMITTED);
    this.jobState.setTasks(3);
    this.jobState.setProp("foo", "bar");
    for (int i = 0; i < 3; i++) {
      WorkUnitState workUnitState = new WorkUnitState();
      workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, "TestJob-1");
      workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, "TestTask-" + i);
      TaskState taskState = new TaskState(workUnitState);
      taskState.setTaskId("TestTask-" + i);
      taskState.setId(taskState.getTaskId());
      taskState.setStartTime(this.startTime);
      taskState.setEndTime(this.startTime + 1000);
      taskState.setTaskDuration(1000);
      taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      taskState.setProp("foo", "bar");
      this.jobState.addTaskState(taskState);
    }

    doAsserts();
  }

  @Test(dependsOnMethods = {"testSetAndGet"})
  public void testSerDe()
      throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
      DataOutputStream dos = closer.register(new DataOutputStream(baos));
      this.jobState.write(dos);

      ByteArrayInputStream bais = closer.register((new ByteArrayInputStream(baos.toByteArray())));
      DataInputStream dis = closer.register((new DataInputStream(bais)));
      JobState newJobState = new JobState();
      newJobState.readFields(dis);

      doAsserts();
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private void doAsserts() {
    Assert.assertEquals(this.jobState.getJobName(), "TestJob");
    Assert.assertEquals(this.jobState.getJobId(), "TestJob-1");
    Assert.assertEquals(this.jobState.getId(), "TestJob-1");
    Assert.assertEquals(this.jobState.getStartTime(), this.startTime);
    Assert.assertEquals(this.jobState.getEndTime(), this.startTime + 1000);
    Assert.assertEquals(this.jobState.getDuration(), 1000);
    Assert.assertEquals(this.jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(this.jobState.getTasks(), 3);
    Assert.assertEquals(this.jobState.getCompletedTasks(), 3);
    Assert.assertEquals(this.jobState.getProp("foo"), "bar");

    List<String> taskStateIds = Lists.newArrayList();
    for (int i = 0; i < this.jobState.getCompletedTasks(); i++) {
      TaskState taskState = this.jobState.getTaskStates().get(i);
      Assert.assertEquals(taskState.getJobId(), "TestJob-1");
      Assert.assertEquals(taskState.getStartTime(), this.startTime);
      Assert.assertEquals(taskState.getEndTime(), this.startTime + 1000);
      Assert.assertEquals(taskState.getTaskDuration(), 1000);
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(taskState.getProp("foo"), "bar");
      taskStateIds.add(taskState.getTaskId());
    }

    Collections.sort(taskStateIds);
    Assert.assertEquals(taskStateIds, Lists.newArrayList("TestTask-0", "TestTask-1", "TestTask-2"));
  }

  @Test(dependsOnMethods = {"testSetAndGet"})
  public void testToJobExecutionInfo() {
    JobExecutionInfo jobExecutionInfo = this.jobState.toJobExecutionInfo();
    Assert.assertEquals(jobExecutionInfo.getJobName(), "TestJob");
    Assert.assertEquals(jobExecutionInfo.getJobId(), "TestJob-1");
    Assert.assertEquals(jobExecutionInfo.getStartTime().longValue(), this.startTime);
    Assert.assertEquals(jobExecutionInfo.getEndTime().longValue(), this.startTime + 1000);
    Assert.assertEquals(jobExecutionInfo.getDuration().longValue(), 1000L);
    Assert.assertEquals(jobExecutionInfo.getState().name(), JobState.RunningState.COMMITTED.name());
    Assert.assertEquals(jobExecutionInfo.getLaunchedTasks().intValue(), 3);
    Assert.assertEquals(jobExecutionInfo.getCompletedTasks().intValue(), 3);
    Assert.assertEquals(jobExecutionInfo.getJobProperties().get("foo"), "bar");

    List<String> taskStateIds = Lists.newArrayList();
    for (TaskExecutionInfo taskExecutionInfo : jobExecutionInfo.getTaskExecutions()) {
      Assert.assertEquals(taskExecutionInfo.getJobId(), "TestJob-1");
      Assert.assertEquals(taskExecutionInfo.getStartTime().longValue(), this.startTime);
      Assert.assertEquals(taskExecutionInfo.getEndTime().longValue(), this.startTime + 1000);
      Assert.assertEquals(taskExecutionInfo.getDuration().longValue(), 1000);
      Assert.assertEquals(taskExecutionInfo.getState().name(), WorkUnitState.WorkingState.COMMITTED.name());
      Assert.assertEquals(taskExecutionInfo.getTaskProperties().get("foo"), "bar");
      taskStateIds.add(taskExecutionInfo.getTaskId());
    }

    Collections.sort(taskStateIds);
    Assert.assertEquals(taskStateIds, Lists.newArrayList("TestTask-0", "TestTask-1", "TestTask-2"));
  }
}
