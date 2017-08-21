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

import org.apache.gobblin.configuration.ConfigurationKeys;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.util.JobLauncherUtils;


/**
 * Unit tests for {@link TaskStateCollectorService}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.runtime" })
public class TaskStateCollectorServiceTest {

  private static final String JOB_NAME = "TestJob";
  private static final String JOB_ID = JobLauncherUtils.newJobId(JOB_NAME);
  private static final String TASK_ID_0 = JobLauncherUtils.newTaskId(JOB_ID, 0);
  private static final String TASK_ID_1 = JobLauncherUtils.newTaskId(JOB_ID, 1);

  private final Path outputTaskStateDir = new Path(TaskStateCollectorServiceTest.class.getSimpleName());

  private FileSystem localFs;

  private FsStateStore<TaskState> taskStateStore;

  private TaskStateCollectorService taskStateCollectorService;

  private final JobState jobState = new JobState();

  private final EventBus eventBus = new EventBus();

  private final Map<String, TaskState> taskStateMap = Maps.newHashMap();

  @BeforeClass
  public void setUp() throws Exception {
    this.localFs = FileSystem.getLocal(new Configuration());
    this.localFs.mkdirs(this.outputTaskStateDir);

    this.taskStateStore = new FsStateStore<>(this.localFs, this.outputTaskStateDir.toUri().getPath(), TaskState.class);


    this.taskStateCollectorService = new TaskStateCollectorService(new Properties(), this.jobState, this.eventBus,
        this.taskStateStore, new Path(this.outputTaskStateDir, JOB_ID));

    this.eventBus.register(this);
  }

  @Test
  public void testPutIntoTaskStateStore() throws IOException {
    TaskState taskState1 = new TaskState();
    taskState1.setJobId(JOB_ID);
    taskState1.setTaskId(TASK_ID_0);
    this.taskStateStore.put(JOB_ID, TASK_ID_0 + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX, taskState1);

    TaskState taskState2 = new TaskState();
    taskState2.setJobId(JOB_ID);
    taskState2.setTaskId(TASK_ID_1);
    this.taskStateStore.put(JOB_ID, TASK_ID_1 + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX, taskState2);
  }

  @Test(dependsOnMethods = "testPutIntoTaskStateStore")
  public void testCollectOutputTaskStates() throws Exception {
    this.taskStateCollectorService.runOneIteration();
    Assert.assertEquals(this.jobState.getTaskStates().size(), 2);
    Assert.assertEquals(this.taskStateMap.size(), 2);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_0).getJobId(), JOB_ID);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_0).getTaskId(), TASK_ID_0);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_1).getJobId(), JOB_ID);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_1).getTaskId(), TASK_ID_1);
  }

  @Test
  public void testHandlerResolution() throws Exception{
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.TASK_STATE_COLLECTOR_HANDLER_CLASS, "hivereg");
    TaskStateCollectorService taskStateCollectorServiceHive = new TaskStateCollectorService(props, this.jobState, this.eventBus,
        this.taskStateStore, new Path(this.outputTaskStateDir, JOB_ID + "_prime"));
    Assert.assertEquals(taskStateCollectorServiceHive.getOptionalTaskCollectorHandler().get().getClass().getName(),
        "org.apache.gobblin.runtime.HiveRegTaskStateCollectorServiceHandlerImpl");
    taskStateCollectorServiceHive.shutDown();
    return;
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.localFs.exists(this.outputTaskStateDir)) {
      this.localFs.delete(this.outputTaskStateDir, true);
    }
  }

  @Subscribe
  @Test(enabled = false)
  public void handleNewOutputTaskStateEvent(NewTaskCompletionEvent newOutputTaskStateEvent) {
    for (TaskState taskState : newOutputTaskStateEvent.getTaskStates()) {
      this.taskStateMap.put(taskState.getTaskId(), taskState);
    }
  }
}
