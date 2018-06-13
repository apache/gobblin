package org.apache.gobblin.cluster;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.testng.Assert;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

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
    }

    @Override
    public Task createNewTask(TaskCallbackContext context) {
      return new TestHelixJobTask(context, this.sysConfig, stateStores);
    }
  }

  public class TestHelixJobTask extends GobblinHelixJobTask {
    public TestHelixJobTask(TaskCallbackContext context,
        Config sysConfig,
        StateStores stateStores) {
      super(context, sysConfig, stateStores);
    }

    //TODO: change below to Helix UserConentStore
    protected void setResultToUserContent(Map<String, String> keyValues) throws IOException {
      super.setResultToUserContent(keyValues);
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
      Assert.assertFalse(PropertiesUtils.getPropAsBoolean(properties, Partitioner.IS_EARLY_STOPPED, "false"));
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
