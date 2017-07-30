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

package gobblin.cluster;

import java.util.Map;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.event.EventName;
import gobblin.runtime.EventMetadataUtils;
import gobblin.runtime.JobContext;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskState;

/**
 * Unit tests for {@link ClusterEventMetadataGenerator}.
 */
@Test(groups = { "gobblin.cluster" })
public class ClusterEventMetadataGeneratorTest {
  public final static Logger LOG = LoggerFactory.getLogger(ClusterEventMetadataGeneratorTest.class);

  @Test
  public void testProcessedCount() throws Exception {
    JobContext jobContext = Mockito.mock(JobContext.class);
    JobState jobState = new JobState("jobName", "1234");
    TaskState taskState1 = new TaskState();
    TaskState taskState2 = new TaskState();

    taskState1.setTaskId("1");
    taskState1.setProp(ConfigurationKeys.WRITER_RECORDS_WRITTEN, "1");
    taskState2.setTaskId("2");
    taskState2.setProp(ConfigurationKeys.WRITER_RECORDS_WRITTEN, "22");

    jobState.addTaskState(taskState1);
    jobState.addTaskState(taskState2);

    Mockito.when(jobContext.getJobState()).thenReturn(jobState);

    ClusterEventMetadataGenerator metadataGenerator = new ClusterEventMetadataGenerator();

    Map<String, String> metadata;

    // processed count is not in job cancel event
    metadata = metadataGenerator.getMetadata(jobContext, EventName.JOB_CANCEL);
    Assert.assertEquals(metadata.get("processedCount"), null);

    // processed count is in job complete event
    metadata = metadataGenerator.getMetadata(jobContext, EventName.getEnumFromEventId("JobCompleteTimer"));
    Assert.assertEquals(metadata.get("processedCount"), "23");
  }

  @Test
  public void testErrorMessage() throws Exception {
    JobContext jobContext = Mockito.mock(JobContext.class);
    JobState jobState = new JobState("jobName", "1234");
    TaskState taskState1 = new TaskState();
    TaskState taskState2 = new TaskState();

    taskState1.setTaskId("1");
    taskState1.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY, "exception1");
    taskState2.setTaskId("2");
    taskState2.setProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY, "exception2");
    taskState2.setProp(EventMetadataUtils.TASK_FAILURE_MESSAGE_KEY, "failureMessage2");

    jobState.addTaskState(taskState1);
    jobState.addTaskState(taskState2);

    Mockito.when(jobContext.getJobState()).thenReturn(jobState);

    ClusterEventMetadataGenerator metadataGenerator = new ClusterEventMetadataGenerator();

    Map<String, String> metadata;

    // error message is not in job commit event
    metadata = metadataGenerator.getMetadata(jobContext, EventName.JOB_COMMIT);
    Assert.assertEquals(metadata.get("message"), null);

    // error message is in job failed event
    metadata = metadataGenerator.getMetadata(jobContext, EventName.JOB_FAILED);
    Assert.assertTrue(metadata.get("message").startsWith("failureMessage"));
    Assert.assertTrue(metadata.get("message").contains("exception1"));
    Assert.assertTrue(metadata.get("message").contains("exception2"));
  }
}
