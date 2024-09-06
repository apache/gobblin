package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecuteGobblinWorkflowImplTest {

  @Test
  public void testCalculateWorkDirsDeletion() {
    String jobId = "jobId";
    Set<String> dirsToDelete = new HashSet<>();
    dirsToDelete.add("/tmp/jobId/task-staging/");
    dirsToDelete.add("/tmp/jobId/task-output/");
    dirsToDelete.add("/tmp/jobId/task-output/file");
    dirsToDelete.add("/tmp/jobId");
    dirsToDelete.add("/tmp");
    Set<String> result = ExecuteGobblinWorkflowImpl.calculateWorkDirsToDelete(jobId, dirsToDelete);
    Assert.assertEquals(result.size(), 4);
    Assert.assertTrue(result.contains("/tmp/jobId/task-output/file"));
    Assert.assertTrue(result.contains("/tmp/jobId/task-output"));
    Assert.assertTrue(result.contains("/tmp/jobId/task-staging"));
    Assert.assertTrue(result.contains("/tmp/jobId"));
  }
}
