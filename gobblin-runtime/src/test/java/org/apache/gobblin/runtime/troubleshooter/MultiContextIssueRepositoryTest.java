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

package org.apache.gobblin.runtime.troubleshooter;

import java.util.List;

import org.testng.annotations.Test;

import org.apache.gobblin.service.ServiceConfigKeys;

import static org.testng.Assert.assertEquals;


public abstract class MultiContextIssueRepositoryTest {

  @Test
  public void canPutIssue()
      throws Exception {
    MultiContextIssueRepository repository = getRepository();

    Issue testIssue = getTestIssue("first", "code1");
    repository.put("job1", testIssue);

    List<Issue> issues = repository.getAll("job1");
    assertEquals(1, issues.size());
    assertEquals(testIssue, issues.get(0));
  }

  @Test
  public void canPutMultipleJobIssue()
      throws Exception {
    MultiContextIssueRepository repository = getRepository();

    repository.put("job1", getTestIssue("first", "code1"));
    repository.put("job1", getTestIssue("second", "code2"));

    List<Issue> issues = repository.getAll("job1");
    assertEquals(2, issues.size());
  }

  @Test
  public void canWorkWithMultipleJobs()
      throws Exception {
    MultiContextIssueRepository repository = getRepository();

    Issue job1Issue1 = getTestIssue("first", "code1");
    Issue job2Issue1 = getTestIssue("first", "code1");
    Issue job2Issue2 = getTestIssue("second", "code2");

    repository.put("job1", job1Issue1);
    repository.put("job2", job2Issue1);
    repository.put("job2", job2Issue2);

    assertEquals(1, repository.getAll("job1").size());
    assertEquals(2, repository.getAll("job2").size());
    assertEquals(2, repository.getAll("job2").size());

    assertEquals(job1Issue1, repository.getAll("job1").get(0));
  }

  @Test
  public void canRemoveIssue()
      throws Exception {
    MultiContextIssueRepository repository = getRepository();

    repository.put("job1", getTestIssue("first", "code1"));
    repository.put("job1", getTestIssue("second", "code2"));

    repository.remove("job1", "code1");
    List<Issue> issues = repository.getAll("job1");
    assertEquals(1, issues.size());
    assertEquals("code2", issues.get(0).getCode());
  }

  @Test
  public void willPreserveIssueInsertionOrder()
      throws Exception {

    int jobCount = 10;
    int issueCount = ServiceConfigKeys.DEFAULT_MEMORY_ISSUE_REPO_MAX_ISSUE_PER_CONTEXT;

    MultiContextIssueRepository repository = getRepository();

    for (int j = 0; j < jobCount; j++) {
      for (int i = 0; i < issueCount; i++) {
        repository.put("job" + j, getTestIssue("issue " + i, String.valueOf(i)));
      }
    }

    for (int j = 0; j < jobCount; j++) {
      List<Issue> retrievedIssues = repository.getAll("job" + j);
      assertEquals(retrievedIssues.size(), issueCount);
      for (int i = 0; i < issueCount; i++) {
        assertEquals(String.valueOf(i), retrievedIssues.get(i).getCode());
      }
    }
  }

  protected abstract MultiContextIssueRepository getRepository();

  protected Issue getTestIssue(String summary, String code) {
    return Issue.builder().summary(summary).code(code).build();
  }
}
