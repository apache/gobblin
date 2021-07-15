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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.avro.Schema;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.example.simplejson.SimpleJsonConverter;
import org.apache.gobblin.example.simplejson.SimpleJsonSource;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.Id;
import org.apache.gobblin.util.SerializationUtils;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;
import org.apache.gobblin.util.eventbus.EventBusFactory;
import org.apache.gobblin.util.retry.RetryerFactory;
import org.apache.gobblin.writer.AvroDataWriterBuilder;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.WriterOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskResult;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import static org.apache.gobblin.cluster.GobblinHelixTaskStateTracker.IS_TASK_METRICS_SCHEDULING_FAILURE_FATAL;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIMES;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TYPE;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link GobblinHelixTask}.
 *
 * <p>
 *   This class uses a mocked {@link HelixManager} to control the behavior of certain method for testing.
 *   A {@link TaskExecutor} is used to run the test task and a {@link GobblinHelixTaskStateTracker} is
 *   also used as being required by {@link GobblinHelixTaskFactory}. The test task writes everything to
 *   the local file system as returned by {@link FileSystem#getLocal(Configuration)}.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.cluster" })
public class GobblinHelixTaskTest {

  private TaskExecutor taskExecutor;

  private GobblinHelixTaskStateTracker taskStateTracker;

  private GobblinHelixTask gobblinHelixTask;

  private HelixManager helixManager;

  private FileSystem localFs;

  private Path appWorkDir;

  private Path taskOutputDir;

  private CountDownLatch countDownLatchForFailInTaskCreation;

  @BeforeClass
  public void setUp() throws IOException {
    Configuration configuration = new Configuration();
    configuration.setInt(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY, 1);
    this.taskExecutor = new TaskExecutor(configuration);

    this.helixManager = Mockito.mock(HelixManager.class);
    when(this.helixManager.getInstanceName()).thenReturn(GobblinHelixTaskTest.class.getSimpleName());
    Properties stateTrackerProp = new Properties();
    stateTrackerProp.setProperty(IS_TASK_METRICS_SCHEDULING_FAILURE_FATAL, "true");
    this.taskStateTracker = new GobblinHelixTaskStateTracker(stateTrackerProp);

    this.localFs = FileSystem.getLocal(configuration);
    this.appWorkDir = new Path(GobblinHelixTaskTest.class.getSimpleName());
    this.taskOutputDir = new Path(this.appWorkDir, "output");
  }

  @Test
  public void testPrepareTask()
      throws IOException, InterruptedException {

    EventBus eventBus = EventBusFactory.get(ContainerHealthCheckFailureEvent.CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME,
        SharedResourcesBrokerFactory.getImplicitBroker());
    eventBus.register(this);

    countDownLatchForFailInTaskCreation = new CountDownLatch(1);

    // Serialize the JobState that will be read later in GobblinHelixTask
    Path jobStateFilePath =
        new Path(appWorkDir, TestHelper.TEST_JOB_ID + "." + AbstractJobLauncher.JOB_STATE_FILE_NAME);
    JobState jobState = new JobState();
    jobState.setJobName(TestHelper.TEST_JOB_NAME);
    jobState.setJobId(TestHelper.TEST_JOB_ID);
    SerializationUtils.serializeState(this.localFs, jobStateFilePath, jobState);

    // Prepare the WorkUnit
    WorkUnit workUnit = WorkUnit.createEmpty();
    prepareWorkUnit(workUnit);

    // Prepare the source Json file
    File sourceJsonFile = new File(this.appWorkDir.toString(), TestHelper.TEST_JOB_NAME + ".json");
    TestHelper.createSourceJsonFile(sourceJsonFile);
    workUnit.setProp(SimpleJsonSource.SOURCE_FILE_KEY, sourceJsonFile.getAbsolutePath());

    // Serialize the WorkUnit into a file
    // expected path is appWorkDir/_workunits/job_id/job_id.wu
    Path workUnitDirPath = new Path(this.appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);
    FsStateStore<WorkUnit> wuStateStore = new FsStateStore<>(this.localFs, workUnitDirPath.toString(), WorkUnit.class);
    Path workUnitFilePath = new Path(new Path(workUnitDirPath, TestHelper.TEST_JOB_ID),
        TestHelper.TEST_JOB_NAME + ".wu");
    wuStateStore.put(TestHelper.TEST_JOB_ID, TestHelper.TEST_JOB_NAME + ".wu", workUnit);

    Assert.assertTrue(this.localFs.exists(workUnitFilePath));

    // Prepare the GobblinHelixTask
    Map<String, String> taskConfigMap = Maps.newHashMap();
    taskConfigMap.put(GobblinClusterConfigurationKeys.WORK_UNIT_FILE_PATH, workUnitFilePath.toString());
    taskConfigMap.put(ConfigurationKeys.JOB_NAME_KEY, TestHelper.TEST_JOB_NAME);
    taskConfigMap.put(ConfigurationKeys.JOB_ID_KEY, TestHelper.TEST_JOB_ID);
    taskConfigMap.put(ConfigurationKeys.TASK_KEY_KEY, Long.toString(Id.parse(TestHelper.TEST_JOB_ID).getSequence()));

    TaskConfig taskConfig = new TaskConfig("", taskConfigMap, true);
    TaskCallbackContext taskCallbackContext = Mockito.mock(TaskCallbackContext.class);
    when(taskCallbackContext.getTaskConfig()).thenReturn(taskConfig);
    when(taskCallbackContext.getManager()).thenReturn(this.helixManager);
    TaskDriver taskDriver = createTaskDriverWithMockedAttributes(taskCallbackContext, taskConfig);

    TaskRunnerSuiteBase.Builder builder = new TaskRunnerSuiteBase.Builder(ConfigFactory.empty()
        .withValue(RETRY_TYPE, ConfigValueFactory.fromAnyRef(RetryerFactory.RetryType.FIXED_ATTEMPT.name()))
        .withValue(RETRY_TIMES, ConfigValueFactory.fromAnyRef(2))
    );
    TaskRunnerSuiteBase sb = builder.setInstanceName("TestInstance")
        .setApplicationName("TestApplication")
        .setAppWorkPath(appWorkDir)
        .setContainerMetrics(Optional.absent())
        .setFileSystem(localFs)
        .setJobHelixManager(this.helixManager)
        .setApplicationId("TestApplication-1")
        .build();

    GobblinHelixTaskFactory gobblinHelixTaskFactory =
        new GobblinHelixTaskFactory(builder,
                                    sb.metricContext,
                                    this.taskStateTracker,
                                    ConfigFactory.empty(),
                                    Optional.of(taskDriver));

    // Expect to go through.
    this.gobblinHelixTask = (GobblinHelixTask) gobblinHelixTaskFactory.createNewTask(taskCallbackContext);

    // Mock the method getFs() which get called in SingleTask constructor, so that SingleTask could fail and trigger retry,
    // which would also fail eventually with timeout.
    TaskRunnerSuiteBase.Builder builderSpy = Mockito.spy(builder);
    when(builderSpy.getFs()).thenThrow(new RuntimeException("failure on purpose"));
    gobblinHelixTaskFactory =
        new GobblinHelixTaskFactory(builderSpy,
            sb.metricContext,
            this.taskStateTracker,
            ConfigFactory.empty(),
            Optional.of(taskDriver));

    // Expecting the eventBus containing the failure signal when run is called
    try {
      gobblinHelixTaskFactory.createNewTask(taskCallbackContext).run();
    } catch (Throwable t){
      return;
    }
    Assert.fail();
  }

  @Subscribe
  @Test(enabled = false)
  // When a class has "@Test" annotation, TestNG will run all public methods as tests.
  // This specific method is public because eventBus is calling it. To prevent running it as a test, we mark it
  // as "disabled" test.
  public void handleContainerHealthCheckFailureEvent(ContainerHealthCheckFailureEvent event) {
    this.countDownLatchForFailInTaskCreation.countDown();
  }

  /**
   * To test against org.apache.gobblin.cluster.GobblinHelixTask#getPartitionForHelixTask(org.apache.helix.task.TaskDriver)
   * we need to assign the right partition id for each helix task, which would be queried from taskDriver.
   * This method encapsulate all mocking steps for taskDriver object to return expected value.
   */
  private TaskDriver createTaskDriverWithMockedAttributes(TaskCallbackContext taskCallbackContext,
      TaskConfig taskConfig) {
    String helixJobId = Joiner.on("_").join(TestHelper.TEST_JOB_ID, TestHelper.TEST_JOB_ID);
    JobConfig jobConfig = Mockito.mock(JobConfig.class);
    when(jobConfig.getJobId()).thenReturn(helixJobId);
    when(taskCallbackContext.getJobConfig()).thenReturn(jobConfig);
    JobContext mockJobContext = Mockito.mock(JobContext.class);
    Map<String, Integer> taskIdPartitionMap = ImmutableMap.of(taskConfig.getId(), 0);
    when(mockJobContext.getTaskIdPartitionMap()).thenReturn(taskIdPartitionMap);
    TaskDriver taskDriver = Mockito.mock(TaskDriver.class);
    when(taskDriver.getJobContext(Mockito.anyString())).thenReturn(mockJobContext);
    return taskDriver;
  }

  @Test(dependsOnMethods = "testPrepareTask")
  public void testRun() throws IOException {
    TaskResult taskResult = this.gobblinHelixTask.run();
    System.out.println(taskResult.getInfo());
    Assert.assertEquals(taskResult.getStatus(), TaskResult.Status.COMPLETED);

    File outputAvroFile = new File(this.taskOutputDir.toString(),
        TestHelper.REL_WRITER_FILE_PATH + File.separator + TestHelper.WRITER_FILE_NAME);
    Assert.assertTrue(outputAvroFile.exists());

    Schema schema = new Schema.Parser().parse(TestHelper.SOURCE_SCHEMA);
    TestHelper.assertGenericRecords(outputAvroFile, schema);
  }

  @AfterClass
  public void tearDown() throws IOException {
    try {
      if (this.localFs.exists(this.appWorkDir)) {
        this.localFs.delete(this.appWorkDir, true);
      }
    } finally {
      this.taskExecutor.stopAsync().awaitTerminated();
      this.taskStateTracker.stopAsync().awaitTerminated();
    }
  }

  private void prepareWorkUnit(WorkUnit workUnit) {
    workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, TestHelper.TEST_TASK_ID);
    workUnit.setProp(ConfigurationKeys.TASK_KEY_KEY, Long.toString(Id.parse(TestHelper.TEST_TASK_ID).getSequence()));
    workUnit.setProp(ConfigurationKeys.SOURCE_CLASS_KEY, SimpleJsonSource.class.getName());
    workUnit.setProp(ConfigurationKeys.CONVERTER_CLASSES_KEY, SimpleJsonConverter.class.getName());
    workUnit.setProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, WriterOutputFormat.AVRO.toString());
    workUnit.setProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, Destination.DestinationType.HDFS.toString());
    workUnit.setProp(ConfigurationKeys.WRITER_STAGING_DIR, this.appWorkDir.toString() + Path.SEPARATOR + "staging");
    workUnit.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, this.taskOutputDir.toString());
    workUnit.setProp(ConfigurationKeys.WRITER_FILE_NAME, TestHelper.WRITER_FILE_NAME);
    workUnit.setProp(ConfigurationKeys.WRITER_FILE_PATH, TestHelper.REL_WRITER_FILE_PATH);
    workUnit.setProp(ConfigurationKeys.WRITER_BUILDER_CLASS, AvroDataWriterBuilder.class.getName());
    workUnit.setProp(ConfigurationKeys.SOURCE_SCHEMA, TestHelper.SOURCE_SCHEMA);
  }
}
