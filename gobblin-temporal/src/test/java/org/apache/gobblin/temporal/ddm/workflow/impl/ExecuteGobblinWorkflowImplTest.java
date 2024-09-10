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

package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecuteGobblinWorkflowImplTest {

  @Test
  public void testCalculateWorkDirsDeletion() throws Exception {
    String jobId = "jobId";
    Set<String> dirsToDelete = new HashSet<>();
    dirsToDelete.add("/tmp/jobId/task-staging/");
    dirsToDelete.add("/tmp/jobId/task-output/");
    dirsToDelete.add("/tmp/jobId/task-output/file");
    dirsToDelete.add("/tmp/jobId/otherDir");
    Set<String> result = ExecuteGobblinWorkflowImpl.calculateWorkDirsToDelete(jobId, dirsToDelete);
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result, dirsToDelete);
  }

  @Test
  public void testThrowsIfNonJobDirInWorkDirs() throws Exception {
    String jobId = "jobId";
    Set<String> dirsToDelete = new HashSet<>();
    dirsToDelete.add("/tmp/jobId/task-staging/");
    dirsToDelete.add("/tmp/jobId/task-output/");
    dirsToDelete.add("/tmp/jobId/task-output/file");
    dirsToDelete.add("/tmp/jobId");
    // Add a non-job dir that should blow up the job
    dirsToDelete.add("/tmp");
    dirsToDelete.add("/sharedDir");

    Assert.assertThrows(IOException.class, () -> ExecuteGobblinWorkflowImpl.calculateWorkDirsToDelete(jobId, dirsToDelete));
    try {
      ExecuteGobblinWorkflowImpl.calculateWorkDirsToDelete(jobId, dirsToDelete);
    } catch (IOException e) {
      Assert.assertEquals(e.getMessage(),
          "Found directories set to delete not associated with job jobId: [/tmp, /sharedDir]. Please validate staging and output directories");
    }
  }
}
