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

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.testng.Assert;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.cluster.suite.IntegrationJobFactorySuite;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.util.PropertiesUtils;


public class TaskRunnerSuiteForJobFactoryTest extends TaskRunnerSuiteThreadModel {
  private TaskFactory testJobFactory;
  public TaskRunnerSuiteForJobFactoryTest(IntegrationJobFactorySuite.TestJobFactorySuiteBuilder builder) {
    super(builder);
    this.testJobFactory = new TestJobFactory(builder);
  }

  @Override
  protected Map<String, TaskFactory> getTaskFactoryMap() {
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();
    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_TASK_FACTORY_NAME, taskFactory);
    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_JOB_FACTORY_NAME, testJobFactory);
    return taskFactoryMap;
  }

  public class TestJobFactory extends GobblinHelixJobFactory {
    public TestJobFactory(IntegrationJobFactorySuite.TestJobFactorySuiteBuilder builder) {
      super (builder);
      this.builder = builder;
    }

    @Override
    public Task createNewTask(TaskCallbackContext context) {
      return new TestHelixJobTask(context, stateStores, builder);
    }
  }

  public class TestHelixJobTask extends GobblinHelixJobTask {
    public TestHelixJobTask(TaskCallbackContext context,
        StateStores stateStores,
        TaskRunnerSuiteBase.Builder builder) {
      super(context, stateStores, builder);
    }

    //TODO: change below to Helix UserConentStore
    protected void setResultToUserContent(Map<String, String> keyValues) throws IOException {
      Map<String, String> customizedKVs = Maps.newHashMap(keyValues);
      customizedKVs.put("customizedKey_1", "customizedVal_1");
      customizedKVs.put("customizedKey_2", "customizedVal_2");
      customizedKVs.put("customizedKey_3", "customizedVal_3");
      super.setResultToUserContent(customizedKVs);
    }
  }

  @Slf4j
  public static class TestDistributedExecutionLauncher extends GobblinHelixDistributeJobExecutionLauncher {

    public TestDistributedExecutionLauncher(GobblinHelixDistributeJobExecutionLauncher.Builder builder) throws Exception {
      super(builder);
    }

    //TODO: change below to Helix UserConentStore
    protected DistributeJobResult getResultFromUserContent() {
      DistributeJobResult rst = super.getResultFromUserContent();
      Properties properties = rst.getProperties().get();
      Assert.assertTrue(properties.containsKey(Partitioner.IS_EARLY_STOPPED));
      Assert.assertFalse(PropertiesUtils.getPropAsBoolean(properties, Partitioner.IS_EARLY_STOPPED, "false"));

      Assert.assertTrue(properties.getProperty("customizedKey_1").equals("customizedVal_1"));
      Assert.assertTrue(properties.getProperty("customizedKey_2").equals("customizedVal_2"));
      Assert.assertTrue(properties.getProperty("customizedKey_3").equals("customizedVal_3"));
      IntegrationJobFactorySuite.completed.set(true);
      return rst;
    }


    @Alias("TestDistributedExecutionLauncherBuilder")
    public static class Builder extends GobblinHelixDistributeJobExecutionLauncher.Builder {
      public TestDistributedExecutionLauncher build() throws Exception {
        return new TestDistributedExecutionLauncher(this);
      }
    }
  }
}
