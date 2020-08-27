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
package org.apache.gobblin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.extractor.InstrumentedExtractor;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.NoopPublisher;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import org.apache.gobblin.runtime.task.TaskFactory;
import org.apache.gobblin.runtime.task.TaskIFace;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.retry.RetryerFactory;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIMES;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TYPE;


@Test (singleThreaded = true)
public class TaskErrorIntegrationTest extends BMNGRunner {
  private static String EXCEPTION_MESSAGE = "test exception";

  @BeforeTest
  @AfterTest
  public void cleanDir()
      throws IOException {
    GobblinLocalJobLauncherUtils.cleanDir();
  }

  /**
   * Test that an extractor that raises an error on creation results in a log message from {@link GobblinMultiTaskAttempt}
   * and does not hang.
   * @throws Exception
   */
  @Test
  public void extractorCreationError()
      throws Exception {
    TestAppender testAppender = new TestAppender();
    Logger logger = LogManager.getLogger(GobblinMultiTaskAttempt.class.getName() + "-noattempt");
    logger.addAppender(testAppender);

    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/skip_workunits_test.properties");

    jobProperties.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, BaseTestSource.class.getName());
    jobProperties.setProperty(TestExtractor.RAISE_ERROR, "true");
    jobProperties.setProperty(RETRY_TYPE, RetryerFactory.RetryType.FIXED_ATTEMPT.name());
    // Disable retry
    jobProperties.setProperty(RETRY_TIMES, "1");

    try {
      GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
    } catch (Exception e){
      // Expecting to get exception, do nothing
    }

    Assert.assertTrue(testAppender.events.stream().anyMatch(e -> e.getRenderedMessage()
        .startsWith("Could not create task for workunit")));

    logger.removeAppender(testAppender);
  }

  /**
   * Test when extractor failure happens as above, retry kicked in and heal the extractor itself.
   */
  @Test
  public void extractorCreationErrorWithRetry() throws Exception {
    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/skip_workunits_test.properties");

    jobProperties.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, BaseTestSource.class.getName());
    jobProperties.setProperty(TestExtractor.RAISE_ERROR, "true");
    jobProperties.setProperty(RETRY_TYPE, RetryerFactory.RetryType.FIXED_ATTEMPT.name());
    jobProperties.setProperty(TestExtractor.ENABLE_RETRY_FLIP, "true");
    // Enable retry and should work for the second time.
    jobProperties.setProperty(RETRY_TIMES, "2");

    // Any failure should fail the test.
    try {
      GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
    } catch (Throwable t) {
      Assert.fail();
    }

    Assert.assertTrue(true);
  }

  /**
   * Test that a task submission error results in a log message from {@link GobblinMultiTaskAttempt}
   * and does not hang
   * @throws Exception
   */
  @Test (enabled = false)
  @BMRule(name = "testErrorDuringSubmission", targetClass = "org.apache.gobblin.runtime.TaskExecutor",
      targetMethod = "submit(Task)", targetLocation = "AT ENTRY", condition = "true",
      action = "throw new RuntimeException(\"Exception for testErrorDuringSubmission\")")
  public void testErrorDuringSubmission()
      throws Exception {
    TestAppender testAppender = new TestAppender();
    Logger logger = LogManager.getLogger(GobblinMultiTaskAttempt.class.getName() + "-noattempt");
    logger.addAppender(testAppender);

    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/skip_workunits_test.properties");

    jobProperties.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, BaseTestSource.class.getName());
    jobProperties.setProperty(TestExtractor.RAISE_ERROR, "false");

    try {
      GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
    } catch (Exception e){
      // Expect to get exception, do nothing
    }

    Assert.assertTrue(testAppender.events.stream().anyMatch(e -> e.getRenderedMessage()
        .startsWith("Could not submit task for workunit")));

    logger.removeAppender(testAppender);
  }

  @Test
  public void testCustomizedTaskFrameworkFailureInTaskCreation() throws Exception {
    TestAppender testAppender = new TestAppender();
    Logger logger = LogManager.getLogger(GobblinMultiTaskAttempt.class.getName() + "-noattempt");
    logger.addAppender(testAppender);

    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/skip_workunits_test.properties");
    jobProperties.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, CustomizedTaskTestSource.class.getName());
    // To demonstrate failure caught in task creation in test setting, disabled retry in task creation.
    jobProperties.setProperty(RETRY_TIMES, "1");
    jobProperties.setProperty(RETRY_TYPE, RetryerFactory.RetryType.FIXED_ATTEMPT.name());

    try {
      GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);
    } catch (Throwable t){
      // Expect to get exception, do nothing
    }
    Assert.assertTrue(testAppender.events.stream().anyMatch(e -> e.getRenderedMessage().contains("Could not create task for workunit")));
    logger.removeAppender(testAppender);
  }


  /**
   * Test extractor that can be configured to raise an exception on construction,
   * or heal it self after even times of retry (constructor-attempt)
   */
  public static class TestExtractor<S, D> extends InstrumentedExtractor<S, D> {
    private static final String RAISE_ERROR = "raiseError";
    private static int RETRY_COUNT = 1;
    private static final String ENABLE_RETRY_FLIP = "enableRetry";

    public TestExtractor(WorkUnitState workUnitState) {
      super(workUnitState);

      try {
        if (workUnitState.getPropAsBoolean(ENABLE_RETRY_FLIP, false) && RETRY_COUNT % 2 == 0) {
          return;
        }

        if (workUnitState.getPropAsBoolean(RAISE_ERROR, false)) {
          throw new RuntimeException(EXCEPTION_MESSAGE);
        }
      } finally {
        // Need to make sure retryCount increment at the end of each constructor.
        RETRY_COUNT += 1;
      }
    }

    @Override
    public S getSchema() throws IOException {
      return null;
    }

    @Override
    public long getExpectedRecordCount() {
      return 0;
    }

    @Override
    public long getHighWatermark() {
      return 0;
    }

    @Override
    public D readRecordImpl(D reuse) throws DataRecordException, IOException {
      return null;
    }
  }

  /**
   * Testing task and factory implementation for Customized Task implementation.
   */
  public static class TestCustomizedTask extends BaseAbstractTask {
    public TestCustomizedTask(TaskContext taskContext) {
      super(taskContext);

      // trigger OutOfMemoryError on purpose during creation phase.
      throw new OutOfMemoryError();
    }
  }

  public static class TestTaskFactory implements TaskFactory {

    @Override
    public TaskIFace createTask(TaskContext taskContext) {
      return new TestCustomizedTask(taskContext);
    }

    @Override
    public DataPublisher createDataPublisher(JobState.DatasetState datasetState) {
      return new NoopPublisher(datasetState);
    }
  }

  public static class CustomizedTaskTestSource extends BaseTestSource {
    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      WorkUnit workUnit = new WorkUnit();
      TaskUtils.setTaskFactoryClass(workUnit, TestTaskFactory.class);
      workUnit.addAll(state);
      return Collections.singletonList(workUnit);
    }
  }


  /**
   * Test source that creates a {@link TestExtractor}
   */
  public static class BaseTestSource implements Source<Schema, GenericRecord> {

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      WorkUnit workUnit = WorkUnit.createEmpty();
      workUnit.addAll(state);
      return Collections.singletonList(workUnit);
    }

    @Override
    public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
        throws IOException {
      return new TestExtractor(state);
    }

    @Override
    public void shutdown(SourceState state) {
    }
  }

  private class TestAppender extends AppenderSkeleton {
    List<LoggingEvent> events = new ArrayList<LoggingEvent>();
    public void close() {}
    public boolean requiresLayout() {return false;}
    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }
  }
}
