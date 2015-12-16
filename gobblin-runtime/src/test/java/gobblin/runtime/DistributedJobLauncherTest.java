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

package gobblin.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.FsStateStore;
import gobblin.source.workunit.WorkUnit;
import gobblin.test.TestSource;
import gobblin.util.JobLauncherUtils;


/**
 * Unit tests for {@link DistributedJobLauncher}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.runtime" })
public class DistributedJobLauncherTest {

  private static final String JOB_NAME = "TestJob";
  private static final String JOB_ID = JobLauncherUtils.newJobId(JOB_NAME);
  private static final String TASK_ID_0 = JobLauncherUtils.newTaskId(JOB_ID, 0);
  private static final String TASK_ID_1 = JobLauncherUtils.newTaskId(JOB_ID, 1);

  private final Path outputTaskStateDir = new Path(DistributedJobLauncherTest.class.getSimpleName());
  private FileSystem localFs;
  private FsStateStore<TaskState> taskStateStore;
  private DistributedJobLauncher distributedJobLauncher;

  private final Map<String, TaskState> taskStateMap = Maps.newHashMap();

  @BeforeClass
  public void setUp() throws Exception {
    this.localFs = FileSystem.getLocal(new Configuration());
    this.localFs.mkdirs(this.outputTaskStateDir);

    this.taskStateStore = new FsStateStore<>(this.localFs, this.outputTaskStateDir.toUri().getPath(), TaskState.class);

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, JOB_NAME);
    properties.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, this.outputTaskStateDir.toUri().getPath());
    properties.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, TestSource.class.getName());
    this.distributedJobLauncher =
        new TestDistributedJobLauncher(properties, this.localFs, ImmutableMap.<String, String>of());
    this.distributedJobLauncher.eventBus.register(this);
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
  public void testCollectOutputTaskStates() throws IOException {
    this.distributedJobLauncher.collectOutputTaskStates(new Path(this.outputTaskStateDir, JOB_ID));
    Assert.assertEquals(this.taskStateMap.size(), 2);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_0).getJobId(), JOB_ID);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_0).getTaskId(), TASK_ID_0);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_1).getJobId(), JOB_ID);
    Assert.assertEquals(this.taskStateMap.get(TASK_ID_1).getTaskId(), TASK_ID_1);
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

  /**
   * Ad dummy test implementation of {@link DistributedJobLauncher}.
   */
  private static class TestDistributedJobLauncher extends DistributedJobLauncher {

    public TestDistributedJobLauncher(Properties jobProps, FileSystem fs, Map<String, String> eventMetadata)
        throws Exception {
      super(jobProps, fs, eventMetadata);
    }

    @Override
    protected void runWorkUnits(List<WorkUnit> workUnits) throws Exception {

    }

    @Override
    protected JobLock getJobLock() throws IOException {
      return null;
    }

    @Override
    protected void executeCancellation() {

    }
  }
}
