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

package org.apache.gobblin.quality;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.qualitychecker.DataQualityStatus;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.service.ServiceConfigKeys;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link DataQualityEvaluator}
 */
@Test(groups = {"gobblin.quality"})
public class DataQualityEvaluatorTest {

  @Test
  public void testDataQualityEvaluationWithAllPassedTasks() {
    // Create job state with properties
    JobState jobState = new JobState("TestJob", "TestJob-1");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "TestFlow");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "TestGroup");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "12345");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "edge1");
    jobState.setProp(ServiceConfigKeys.GOBBLIN_SERVICE_INSTANCE_NAME, "test-instance");
    jobState.setProp(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "test-executor");
    jobState.setProp(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, "test-source");
    jobState.setProp(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, "test-destination");

    // Create task states with PASSED quality status
    List<TaskState> taskStates = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      TaskState taskState = new TaskState();
      taskState.setTaskId("Task-" + i);
      taskState.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.PASSED.name());
      taskStates.add(taskState);
    }

    // Evaluate data quality
    DataQualityEvaluator.DataQualityEvaluationResult result =
        DataQualityEvaluator.evaluateDataQuality(taskStates, jobState);

    // Verify results
    Assert.assertEquals(result.getQualityStatus(), DataQualityStatus.PASSED);
    Assert.assertEquals(result.getTotalFiles(), 3);
    Assert.assertEquals(result.getPassedFiles(), 3);
    Assert.assertEquals(result.getFailedFiles(), 0);
    Assert.assertEquals(result.getNonEvaluatedFiles(), 0);
  }

  @Test
  public void testDataQualityEvaluationWithMixedResults() {
    // Create job state
    JobState jobState = new JobState("TestJob", "TestJob-1");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "TestFlow");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "TestGroup");

    // Create task states with mixed quality status
    List<TaskState> taskStates = new ArrayList<>();

    // Passed task
    TaskState passedTask = new TaskState();
    passedTask.setTaskId("Task-1");
    passedTask.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.PASSED.name());
    taskStates.add(passedTask);

    // Failed task
    TaskState failedTask = new TaskState();
    failedTask.setTaskId("Task-2");
    failedTask.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.FAILED.name());
    taskStates.add(failedTask);

    // Not evaluated task
    TaskState notEvaluatedTask = new TaskState();
    notEvaluatedTask.setTaskId("Task-3");
    notEvaluatedTask.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.NOT_EVALUATED.name());
    taskStates.add(notEvaluatedTask);

    // Evaluate data quality
    DataQualityEvaluator.DataQualityEvaluationResult result =
        DataQualityEvaluator.evaluateDataQuality(taskStates, jobState);

    // Verify results
    Assert.assertEquals(result.getQualityStatus(), DataQualityStatus.FAILED);
    Assert.assertEquals(result.getTotalFiles(), 3);
    Assert.assertEquals(result.getPassedFiles(), 1);
    Assert.assertEquals(result.getFailedFiles(), 1);
    Assert.assertEquals(result.getNonEvaluatedFiles(), 1);
  }

  @Test
  public void testJobPropertiesRetrieval() {
    // Create job state with various properties
    JobState jobState = new JobState("TestJob", "TestJob-1");
    jobState.setProp("test.property1", "value1");
    jobState.setProp("test.property2", "value2");
    jobState.setProp(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, "test-source");
    jobState.setProp(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, "test-destination");

    // Get properties directly from JobState
    Properties jobProperties = jobState.getProperties();

    // Verify properties are correctly retrieved
    Assert.assertEquals(jobProperties.getProperty("test.property1"), "value1");
    Assert.assertEquals(jobProperties.getProperty("test.property2"), "value2");
    Assert.assertEquals(jobProperties.getProperty(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY), "test-source");
    Assert.assertEquals(jobProperties.getProperty(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY),
        "test-destination");
  }

  @Test
  public void testDatasetQualityEvaluation() {
    // Create job state
    JobState jobState = new JobState("TestJob", "TestJob-1");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "TestFlow");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "TestGroup");

    // Create dataset state
    JobState.DatasetState datasetState = new JobState.DatasetState("TestJob", "TestJob-1");
    datasetState.setDatasetUrn("test://dataset/urn");

    // Create task states
    List<TaskState> taskStates = new ArrayList<>();
    TaskState taskState = new TaskState();
    taskState.setTaskId("Task-1");
    taskState.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.PASSED.name());
    taskStates.add(taskState);

    // Add task states to dataset state
    for (TaskState ts : taskStates) {
      datasetState.addTaskState(ts);
    }
    // Evaluate dataset quality
    DataQualityEvaluator.DataQualityEvaluationResult result =
        DataQualityEvaluator.evaluateAndReportDatasetQuality(datasetState, jobState);

    // Verify results
    Assert.assertEquals(result.getQualityStatus(), DataQualityStatus.PASSED);
    Assert.assertEquals(result.getTotalFiles(), 1);
    Assert.assertEquals(result.getPassedFiles(), 1);
    Assert.assertEquals(result.getFailedFiles(), 0);
    Assert.assertEquals(result.getNonEvaluatedFiles(), 0);

    // Verify dataset state was updated
    //Assert.assertEquals(datasetState.getDataQualityStatus(), DataQualityStatus.PASSED);
  }

  @Test
  public void testDataQualityEvaluationWithNullTaskStates() {
    // Create job state
    JobState jobState = new JobState("TestJob", "TestJob-1");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "TestFlow");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "TestGroup");

    // Create task states with some null entries
    List<TaskState> taskStates = new ArrayList<>();

    // Valid passed task
    TaskState passedTask = new TaskState();
    passedTask.setTaskId("Task-1");
    passedTask.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.PASSED.name());
    taskStates.add(passedTask);

    // Null task state
    taskStates.add(null);

    // Valid failed task
    TaskState failedTask = new TaskState();
    failedTask.setTaskId("Task-3");
    failedTask.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.FAILED.name());
    taskStates.add(failedTask);

    // Another null task state
    taskStates.add(null);

    // Valid not evaluated task
    TaskState notEvaluatedTask = new TaskState();
    notEvaluatedTask.setTaskId("Task-5");
    notEvaluatedTask.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_RESULT_KEY, DataQualityStatus.NOT_EVALUATED.name());
    taskStates.add(notEvaluatedTask);

    // Evaluate data quality
    DataQualityEvaluator.DataQualityEvaluationResult result =
        DataQualityEvaluator.evaluateDataQuality(taskStates, jobState);

    // Verify results - should handle null task states gracefully
    // The method should process only non-null task states
    Assert.assertEquals(result.getQualityStatus(), DataQualityStatus.FAILED);
    Assert.assertEquals(result.getTotalFiles(), 5); // Total includes null entries
    Assert.assertEquals(result.getPassedFiles(), 1);
    Assert.assertEquals(result.getFailedFiles(), 1);
    Assert.assertEquals(result.getNonEvaluatedFiles(), 3);
  }

  @Test
  public void testDataQualityEvaluationWithAllNullTaskStates() {
    // Create job state
    JobState jobState = new JobState("TestJob", "TestJob-1");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "TestFlow");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "TestGroup");

    // Create list with all null task states
    List<TaskState> taskStates = new ArrayList<>();
    taskStates.add(null);
    taskStates.add(null);
    taskStates.add(null);

    // Evaluate data quality
    DataQualityEvaluator.DataQualityEvaluationResult result =
        DataQualityEvaluator.evaluateDataQuality(taskStates, jobState);

    // Verify results - should handle all null task states gracefully
    Assert.assertEquals(result.getQualityStatus(), DataQualityStatus.PASSED); // Default status when no failures
    Assert.assertEquals(result.getTotalFiles(), 3);
    Assert.assertEquals(result.getPassedFiles(), 0);
    Assert.assertEquals(result.getFailedFiles(), 0);
    Assert.assertEquals(result.getNonEvaluatedFiles(), 3);
  }

  @Test
  public void testDataQualityEvaluationWithEmptyTaskStatesList() {
    // Create job state
    JobState jobState = new JobState("TestJob", "TestJob-1");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "TestFlow");
    jobState.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "TestGroup");

    // Create empty task states list
    List<TaskState> taskStates = new ArrayList<>();

    // Evaluate data quality
    DataQualityEvaluator.DataQualityEvaluationResult result =
        DataQualityEvaluator.evaluateDataQuality(taskStates, jobState);

    // Verify results - should handle empty list gracefully
    Assert.assertEquals(result.getQualityStatus(), DataQualityStatus.PASSED); // Default status when no tasks
    Assert.assertEquals(result.getTotalFiles(), 0);
    Assert.assertEquals(result.getPassedFiles(), 0);
    Assert.assertEquals(result.getFailedFiles(), 0);
    Assert.assertEquals(result.getNonEvaluatedFiles(), 0);
  }
}
