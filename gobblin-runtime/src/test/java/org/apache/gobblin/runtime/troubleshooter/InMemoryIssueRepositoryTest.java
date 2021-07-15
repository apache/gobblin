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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class InMemoryIssueRepositoryTest {

  @Test
  public void canPutIssue()
      throws Exception {

    InMemoryIssueRepository repository = new InMemoryIssueRepository();

    Issue testIssue = getTestIssue("first", "code1");
    repository.put(testIssue);

    List<Issue> issues = repository.getAll();
    assertEquals(1, issues.size());
    assertEquals(testIssue, issues.get(0));
  }

  @Test
  public void canPutMultipleIssues()
      throws Exception {

    InMemoryIssueRepository repository = new InMemoryIssueRepository();

    repository.put(getTestIssue("first", "code1"));
    repository.put(getTestIssue("second", "code2"));
    repository.put(getTestIssue("third", "code3"));

    List<Issue> issues = repository.getAll();
    assertEquals(3, issues.size());
    assertTrue(issues.stream().anyMatch(i -> i.getCode().equals("code2")));
  }

  @Test
  public void canRemoveIssue()
      throws Exception {

    InMemoryIssueRepository repository = new InMemoryIssueRepository();

    repository.put(getTestIssue("first", "code1"));
    repository.put(getTestIssue("second", "code2"));
    repository.put(getTestIssue("third", "code3"));

    List<Issue> issues = repository.getAll();
    assertEquals(3, issues.size());

    repository.remove("code2");

    issues = repository.getAll();
    assertEquals(2, issues.size());
  }

  @Test
  public void canDeduplicateIssues()
      throws Exception {

    InMemoryIssueRepository repository = new InMemoryIssueRepository();

    repository.put(getTestIssue("first", "code1"));
    repository.put(getTestIssue("second", "code2"));
    repository.put(getTestIssue("second-2", "code2"));
    repository.put(getTestIssue("second-3", "code2"));

    List<Issue> issues = repository.getAll();
    assertEquals(2, issues.size());
    assertTrue(issues.stream().anyMatch(i -> i.getCode().equals("code1")));
    assertTrue(issues.stream().anyMatch(i -> i.getCode().equals("code2")));
  }

  @Test
  public void willIgnoreOverflowIssues()
      throws Exception {
    InMemoryIssueRepository repository = new InMemoryIssueRepository(50);

    for (int i = 0; i < 100; i++) {
      repository.put(getTestIssue("issue " + i, "code" + i));
    }

    assertEquals(50, repository.getAll().size());
  }

  @Test
  public void willPreserveIssueInsertionOrder()
      throws Exception {

    int issueCount = 50;
    InMemoryIssueRepository repository = new InMemoryIssueRepository(issueCount * 2);

    for (int i = 0; i < issueCount; i++) {
      repository.put(getTestIssue("issue " + i, String.valueOf(i)));
    }

    List<Issue> retrievedIssues = repository.getAll();

    for (int i = 0; i < issueCount; i++) {
      assertEquals(String.valueOf(i), retrievedIssues.get(i).getCode());
    }
  }

  private Issue getTestIssue(String summary, String code) {
    return Issue.builder().summary(summary).code(code).build();
  }
}