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

package org.apache.gobblin.temporal.joblauncher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.Path;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.example.simplejson.SimpleJsonSource;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.locks.FileBasedJobLock;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory;
import org.apache.gobblin.temporal.workflows.service.ManagedWorkflowServiceStubs;
import org.apache.gobblin.util.JobLauncherUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class GobblinTemporalJobLauncherTest {

  private GobblinTemporalJobLauncherForTest jobLauncher;
  private MockedStatic<TemporalWorkflowClientFactory> mockWorkflowClientFactory;
  private WorkflowServiceStubs mockServiceStubs;
  private WorkflowClient mockClient;
  private WorkflowStub mockStub;
  private WorkflowExecutionInfo mockExecutionInfo;
  private Properties jobProperties;

  class GobblinTemporalJobLauncherForTest extends GobblinTemporalJobLauncher {
    int cleanupStagingDirectoryCallCount = 0;

    public GobblinTemporalJobLauncherForTest(Properties jobProperties, Path appWorkDir) throws Exception {
      super(jobProperties, appWorkDir, new ArrayList<>(), new ConcurrentHashMap<>(), null);
    }

    @Override
    protected void submitJob(List<WorkUnit> workUnits)
        throws Exception {
      this.workflowId = "someWorkflowId";
    }

    @Override
    protected void cleanupStagingDirectory(JobState jobState) throws IOException {
      cleanupStagingDirectoryCallCount++;
      super.cleanupStagingDirectory(jobState);
    }

    // Expose jobContext for testing
    public org.apache.gobblin.runtime.JobContext getJobContext() {
      return this.jobContext;
    }
  }


  @BeforeClass
  public void setUp() throws Exception {
    mockServiceStubs = mock(WorkflowServiceStubs.class);
    ManagedWorkflowServiceStubs managedWorkflowServiceStubs = new ManagedWorkflowServiceStubs(mockServiceStubs);
    mockClient = mock(WorkflowClient.class);
    mockExecutionInfo = mock(WorkflowExecutionInfo.class);
    DescribeWorkflowExecutionResponse mockResponse = mock(DescribeWorkflowExecutionResponse.class);
    WorkflowServiceGrpc.WorkflowServiceBlockingStub mockBlockingStub = mock(WorkflowServiceGrpc.WorkflowServiceBlockingStub.class);
    when(mockServiceStubs.blockingStub()).thenReturn(mockBlockingStub);
    when(mockBlockingStub.describeWorkflowExecution(Mockito.any())).thenReturn(mockResponse);
    when(mockResponse.getWorkflowExecutionInfo()).thenReturn(mockExecutionInfo);

    mockWorkflowClientFactory = Mockito.mockStatic(TemporalWorkflowClientFactory.class);
    mockWorkflowClientFactory.when(() -> TemporalWorkflowClientFactory.createServiceInstance(Mockito.anyString()))
        .thenReturn(managedWorkflowServiceStubs);
    mockWorkflowClientFactory.when(() -> TemporalWorkflowClientFactory.createClientInstance(Mockito.any(), Mockito.anyString()))
        .thenReturn(mockClient);

    jobProperties = new Properties();
    jobProperties.setProperty(ConfigurationKeys.FS_URI_KEY, "file:///");
    jobProperties.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_CONNECTION_STRING, "someConnString");
    jobProperties.setProperty(ConfigurationKeys.JOB_LOCK_TYPE, FileBasedJobLock.class.getName());
    jobProperties.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, SimpleJsonSource.class.getName());
  }

  @BeforeMethod
  public void methodSetUp() throws Exception {
    mockStub = mock(WorkflowStub.class);
    when(mockClient.newUntypedWorkflowStub(Mockito.anyString())).thenReturn(mockStub);
    when(mockStub.getExecution()).thenReturn(WorkflowExecution.getDefaultInstance());

    File tmpDir = Files.createTempDir();
    String basePath = tmpDir.getAbsolutePath();
    Path appWorkDir = new Path(basePath, "testAppWorkDir");
    String jobLockDir = new Path(basePath, "jobLockDir").toString();
    String stateStoreDir = new Path(basePath, "stateStoreDir").toString();
    String jobName = "testJob";
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProperties.setProperty(ConfigurationKeys.JOB_NAME_KEY, jobName);
    jobProperties.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
    jobProperties.setProperty(FileBasedJobLock.JOB_LOCK_DIR, jobLockDir);
    jobProperties.setProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, stateStoreDir);

    jobLauncher = new GobblinTemporalJobLauncherForTest(jobProperties, appWorkDir);
  }

  @Test
  public void testCancelWorkflowIfFailed() throws Exception {
    // For workflowId to be generated
    jobLauncher.submitJob(null);

    // Mock the workflow status to be failed
    when(mockExecutionInfo.getStatus())
        .thenReturn(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED);

    jobLauncher.executeCancellation();

    verify(mockStub, times(0)).cancel();
  }

  @Test
  public void testCancelWorkflowIfCompleted() throws Exception {
    // For workflowId to be generated
    jobLauncher.submitJob(null);

    // Mock the workflow status to be completed
    when(mockExecutionInfo.getStatus())
        .thenReturn(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED);

    jobLauncher.executeCancellation();

    verify(mockStub, times(0)).cancel();
  }

  @Test
  public void testCancelWorkflowIfRunning() throws Exception {
    // Mock the workflow status to be running
    when(mockExecutionInfo.getStatus())
        .thenReturn(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING);

    jobLauncher.executeCancellation();

    // Verify that the cancel method was not called without job submission
    verify(mockStub, times(0)).cancel();

    jobLauncher.submitJob(null);

    jobLauncher.executeCancellation();

    verify(mockStub, times(1)).cancel();
  }

  @Test
  public void testCancelWorkflowFetchStatusThrowsException() throws Exception {
    // Mock the get workflow status to throw an exception
    Mockito.doThrow(new RuntimeException("Some exception occurred")).when(mockExecutionInfo).getStatus();

    jobLauncher.submitJob(null);

    jobLauncher.executeCancellation();

    verify(mockStub, times(1)).cancel();

    Mockito.reset(mockExecutionInfo);
  }

  @Test
  public void testTerminateWorkflow() throws Exception {
    // Mock the workflow status to be running
    when(mockExecutionInfo.getStatus())
        .thenReturn(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING);

    // Mock getResult to throw TimeoutException
    Mockito.doThrow(new TimeoutException("Workflow still in running"))
        .when(mockStub).getResult(3L, TimeUnit.SECONDS, String.class, String.class);

    jobLauncher.submitJob(null);

    jobLauncher.executeCancellation();

    verify(mockStub, times(1)).terminate("Job cancel invoked");
  }

  @Test
  public void testCleanupStagingDirectoryWithPathsToDelete() throws Exception {
    // Create temp directories to simulate work directories
    File tmpDir = Files.createTempDir();
    File stagingDir = new File(tmpDir, "staging");
    File outputDir = new File(tmpDir, "output");
    stagingDir.mkdirs();
    outputDir.mkdirs();

    // Set up job state with WORK_DIR_PATHS_TO_DELETE
    JobState jobState = jobLauncher.getJobContext().getJobState();
    String pathsToDelete = stagingDir.getAbsolutePath() + "," + outputDir.getAbsolutePath();
    jobState.setProp(GobblinTemporalConfigurationKeys.WORK_DIR_PATHS_TO_DELETE, pathsToDelete);
    jobState.setProp(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, "true");

    // Verify directories exist before cleanup
    assertTrue(stagingDir.exists(), "Staging directory should exist before cleanup");
    assertTrue(outputDir.exists(), "Output directory should exist before cleanup");

    // Execute cleanup
    jobLauncher.cleanupStagingDirectory(jobState);

    // Verify directories are deleted
    assertFalse(stagingDir.exists(), "Staging directory should be deleted after cleanup");
    assertFalse(outputDir.exists(), "Output directory should be deleted after cleanup");

    // Cleanup test directory
    tmpDir.delete();
  }

  @Test
  public void testCleanupStagingDirectoryWithoutPaths() throws Exception {
    // Set up job state WITHOUT WORK_DIR_PATHS_TO_DELETE
    JobState jobState = jobLauncher.getJobContext().getJobState();
    jobState.setProp(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, "true");

    // Should not throw exception when no paths configured
    jobLauncher.cleanupStagingDirectory(jobState);
  }

  @Test
  public void testCleanupStagingDirectoryWithCleanupDisabled() throws Exception {
    // Create temp directories
    File tmpDir = Files.createTempDir();
    File stagingDir = new File(tmpDir, "staging");
    stagingDir.mkdirs();

    // Set up job state with cleanup disabled
    JobState jobState = jobLauncher.getJobContext().getJobState();
    String pathsToDelete = stagingDir.getAbsolutePath();
    jobState.setProp(GobblinTemporalConfigurationKeys.WORK_DIR_PATHS_TO_DELETE, pathsToDelete);
    jobState.setProp(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, "false");

    // Execute cleanup
    jobLauncher.cleanupStagingDirectory(jobState);

    // Verify directory still exists (cleanup was disabled)
    assertTrue(stagingDir.exists(), "Staging directory should still exist when cleanup is disabled");

    // Cleanup
    stagingDir.delete();
    tmpDir.delete();
  }

  @Test
  public void testCloseTriggersCleanup() throws Exception {
    File tmpDir = Files.createTempDir();
    File stagingDir = new File(tmpDir, "staging");
    stagingDir.mkdirs();

    JobState jobState = jobLauncher.getJobContext().getJobState();
    jobState.setProp(GobblinTemporalConfigurationKeys.WORK_DIR_PATHS_TO_DELETE, stagingDir.getAbsolutePath());
    jobState.setProp(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, "true");

    assertTrue(stagingDir.exists(), "Staging directory should exist before close()");

    jobLauncher.close();

    assertFalse(stagingDir.exists(), "close() should trigger cleanup and delete the staging directory");
    tmpDir.delete();
  }

  @Test
  public void testCleanupRunsOnlyOnce() throws Exception {
    File tmpDir = Files.createTempDir();
    File stagingDir = new File(tmpDir, "staging");
    stagingDir.mkdirs();

    JobState jobState = jobLauncher.getJobContext().getJobState();
    jobState.setProp(GobblinTemporalConfigurationKeys.WORK_DIR_PATHS_TO_DELETE, stagingDir.getAbsolutePath());
    jobState.setProp(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_WORK_DIR_CLEANUP_ENABLED, "true");

    // First cleanup - triggered by close()
    jobLauncher.close();
    assertEquals(jobLauncher.cleanupStagingDirectoryCallCount, 1, "Cleanup should run exactly once after close()");

    // Second trigger - simulates the shutdown hook firing after close() already ran
    jobLauncher.triggerCleanupForTest();
    assertEquals(jobLauncher.cleanupStagingDirectoryCallCount, 1, "Cleanup should not run a second time");

    tmpDir.delete();
  }
}