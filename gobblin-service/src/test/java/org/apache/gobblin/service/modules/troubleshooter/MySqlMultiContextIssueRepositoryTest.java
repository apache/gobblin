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

package org.apache.gobblin.service.modules.troubleshooter;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.TestServiceDatabaseConfig;
import org.apache.gobblin.service.modules.db.ServiceDatabaseManager;
import org.apache.gobblin.service.modules.db.ServiceDatabaseProviderImpl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class MySqlMultiContextIssueRepositoryTest {
  private int testId = 1;
  private MySQLContainer mysql;
  private ServiceDatabaseProviderImpl databaseProvider;
  private ServiceDatabaseManager databaseManager;
  private MySqlMultiContextIssueRepository repository;

  @BeforeMethod
  public void setup() {
    testId++;
  }

  @BeforeClass
  public void classSetUp() {
    mysql = new MySQLContainer("mysql:" + TestServiceDatabaseConfig.MysqlVersion);
    mysql.start();

    ServiceDatabaseProviderImpl.Configuration dbConfig =
        ServiceDatabaseProviderImpl.Configuration.builder().url(mysql.getJdbcUrl()).userName(mysql.getUsername())
            .password(mysql.getPassword()).build();

    databaseProvider = new ServiceDatabaseProviderImpl(dbConfig);

    databaseManager = new ServiceDatabaseManager(databaseProvider);
    databaseManager.startAsync().awaitRunning();
    repository = new MySqlMultiContextIssueRepository(databaseProvider);
  }

  @AfterClass
  public void classTearDown() {
    databaseManager.stopAsync().awaitTerminated();
    mysql.stop();
  }

  @Test
  public void canReadEmptyRepository()
      throws Exception {
    List<Issue> issues = repository.getAll("test-nonexistent");
    assertThat(issues).isEmpty();
  }

  @Test
  public void canCreateWithEmptyConfiguration()
      throws Exception {
    MySqlMultiContextIssueRepository.Configuration configuration =
        new MySqlMultiContextIssueRepository.Configuration(ConfigFactory.empty());

    MySqlMultiContextIssueRepository newRepo = new MySqlMultiContextIssueRepository(databaseProvider, configuration);

    newRepo.startAsync().awaitRunning();

    List<Issue> issues = newRepo.getAll("test-nonexistent");
    assertThat(issues).isEmpty();

    newRepo.stopAsync().awaitTerminated();
  }

  @Test
  public void canPutAndGetFullIssue()
      throws Exception {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("test.prop1", "test value 1");
    properties.put("test.prop2", "test value 2");

    // Mysql date has less precision than Java date, so we zero sub-second component of the date to get the same
    // value after retrieval from db

    Issue issue = Issue.builder().summary("Test summary \" ' -- ").code("CODE1")
        .time(ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC)).severity(IssueSeverity.ERROR)
        .details("details for test issue").exceptionClass("java.io.IOException")
        .sourceClass("org.apache.gobblin.service.modules.troubleshooter.AutoTroubleshooterLogAppender")
        .properties(properties).build();

    String contextId = "context-" + testId;
    repository.put(contextId, issue);

    List<Issue> issues = repository.getAll(contextId);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0)).usingRecursiveComparison().isEqualTo(issue);
  }

  @Test
  public void canPutAndGetMinimalIssue()
      throws Exception {
    Issue issue = Issue.builder().summary("Test summary").code("CODE1")
        .time(ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC)).severity(IssueSeverity.WARN).build();

    String contextId = "context-" + testId;
    repository.put(contextId, issue);

    List<Issue> issues = repository.getAll(contextId);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0)).usingRecursiveComparison().isEqualTo(issue);
  }

  @Test
  public void canPutIssueWithMaximumFieldLengths()
      throws Exception {
    // summary and details are bounded at 16MB, so we just put reasonably large values there
    Issue issue = Issue.builder().summary(StringUtils.repeat("s", 100000)).details(StringUtils.repeat("s", 100000))
        .code(StringUtils.repeat("c", Issue.MAX_ISSUE_CODE_LENGTH))
        .exceptionClass(StringUtils.repeat("e", Issue.MAX_CLASSNAME_LENGTH))
        .sourceClass(StringUtils.repeat("s", Issue.MAX_CLASSNAME_LENGTH))
        .time(ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC)).severity(IssueSeverity.WARN).build();

    String contextId = TroubleshooterUtils
        .getContextIdForJob(StringUtils.repeat("g", ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH),
                            StringUtils.repeat("f", ServiceConfigKeys.MAX_FLOW_NAME_LENGTH),
                            String.valueOf(Long.MAX_VALUE),
                            StringUtils.repeat("j", ServiceConfigKeys.MAX_JOB_NAME_LENGTH));

    repository.put(contextId, issue);

    List<Issue> issues = repository.getAll(contextId);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0)).usingRecursiveComparison().isEqualTo(issue);
  }

  @Test
  public void willGetMeaningfulErrorOnOversizedData()
      throws Exception {
    Issue issue = Issue.builder().summary("Test summary").code(StringUtils.repeat("c", Issue.MAX_ISSUE_CODE_LENGTH * 2))
        .time(ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC)).severity(IssueSeverity.WARN).build();

    String contextId = "context-" + testId;

    assertThatThrownBy(() -> {
      repository.put(contextId, issue);
    }).isInstanceOf(TroubleshooterException.class).getRootCause()
        .hasMessageContaining("Data too long for column 'code'");
  }

  @Test
  public void willRollbackWhenSomeIssuesAreInvalid()
      throws Exception {
    Issue validIssue = getTestIssue("test", "test1");
    Issue invalidIssue =
        Issue.builder().summary("Test summary").code(StringUtils.repeat("c", Issue.MAX_ISSUE_CODE_LENGTH * 2))
            .time(ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC)).severity(IssueSeverity.WARN)
            .build();

    String contextId = "context-" + testId;

    try {
      repository.put(contextId, Arrays.asList(validIssue, invalidIssue));
    } catch (TroubleshooterException ex) {
      // exception is expected
    }
    List<Issue> issues = repository.getAll(contextId);

    assertThat(issues).isEmpty();
  }

  @Test
  public void canPutIssueRepeatedly()
      throws Exception {
    Issue issue = getTestIssue("test", "test1");

    String contextId = "context-" + testId;

    repository.put(contextId, issue);
    repository.put(contextId, issue);

    List<Issue> issues = repository.getAll(contextId);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0)).usingRecursiveComparison().isEqualTo(issue);
  }

  @Test
  public void canPutAndGetMultipleIssues()
      throws Exception {
    Issue issue1 = getTestIssue("test-1", "code1");
    Issue issue2 = getTestIssue("test-2", "code2");
    Issue issue3 = getTestIssue("test-3", "code3");
    repository.put("context-1-" + testId, issue1);
    repository.put("context-1-" + testId, issue2);

    repository.put("context-2-" + testId, issue2);
    repository.put("context-2-" + testId, issue3);

    List<Issue> context1Issues = repository.getAll("context-1-" + testId);
    assertThat(context1Issues).hasSize(2);
    assertThat(context1Issues.get(0)).usingRecursiveComparison().isEqualTo(issue1);
    assertThat(context1Issues.get(1)).usingRecursiveComparison().isEqualTo(issue2);

    List<Issue> context2Issues = repository.getAll("context-2-" + testId);
    assertThat(context2Issues).hasSize(2);
    assertThat(context2Issues.get(0)).usingRecursiveComparison().isEqualTo(issue2);
    assertThat(context2Issues.get(1)).usingRecursiveComparison().isEqualTo(issue3);
  }

  @Test
  public void canRemoveIssue()
      throws Exception {
    Issue issue1 = getTestIssue("test-1", "code1");
    Issue issue2 = getTestIssue("test-2", "code2");
    Issue issue3 = getTestIssue("test-3", "code3");

    String contextId = "context-1-" + testId;
    repository.put(contextId, issue1);
    repository.put(contextId, issue2);
    repository.put(contextId, issue3);

    repository.remove(contextId, issue2.getCode());

    List<Issue> issues = repository.getAll(contextId);
    assertThat(issues).hasSize(2);
    assertThat(issues.get(0)).usingRecursiveComparison().isEqualTo(issue1);
    assertThat(issues.get(1)).usingRecursiveComparison().isEqualTo(issue3);
  }

  @Test
  public void willPreserveIssueOrder()
      throws Exception {
    Random random = new Random(1);

    List<Issue> issues = new ArrayList<>();

    String contextId = "context-" + testId;
    for (int i = 0; i < 100; i++) {
      Issue issue = getTestIssue("test-" + random.nextInt(), "code-" + random.nextInt());
      issues.add(issue);
      repository.put(contextId, issue);
    }

    List<Issue> retrievedIssues = repository.getAll(contextId);
    assertThat(retrievedIssues).usingRecursiveComparison().isEqualTo(issues);
  }

  @Test
  public void canRemoveIssuesAboveSpecifiedCount()
      throws Exception {
    String contextId = "context-" + testId;
    for (int i = 0; i < 100; i++) {
      Issue issue = getTestIssue("test-" + i, "code-" + i);
      repository.put(contextId, issue);
    }

    repository.deleteOldIssuesOverTheCount(20);

    List<Issue> retrievedIssues = repository.getAll(contextId);
    assertThat(retrievedIssues).hasSize(20);
    assertThat(retrievedIssues.get(0).getCode()).isEqualTo("code-80");
    assertThat(retrievedIssues.get(19).getCode()).isEqualTo("code-99");
  }

  @Test
  public void canRemoveOlderIssues()
      throws Exception {
    String contextId = "context-" + testId;
    int issueCount = 100;
    ZonedDateTime startTime = ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC);
    for (int i = 0; i < 100; i++) {
      Issue issue = Issue.builder().summary("test summary").code("code-" + i)
          .time(startTime.minus(Duration.ofDays(issueCount - i))).severity(IssueSeverity.ERROR).build();
      repository.put(contextId, issue);
    }

    repository.deleteIssuesOlderThan(startTime.minus(Duration.ofDays(20).plus(Duration.ofHours(1))));

    List<Issue> retrievedIssues = repository.getAll(contextId);
    assertThat(retrievedIssues).hasSize(20);
  }

  @Test(enabled = false) // Load test takes several minutes to run and is disabled by default
  public void canWriteLotsOfIssuesConcurrently()
      throws Exception {
    canWriteLotsOfIssuesConcurrently(false);
  }

  @Test(enabled = false) // Load test takes several minutes to run and is disabled by default
  public void canWriteLotsOfIssuesConcurrentlyWithBatching()
      throws Exception {
    canWriteLotsOfIssuesConcurrently(true);
  }

  private void canWriteLotsOfIssuesConcurrently(boolean useBatching)
      throws Exception {
    int threadCount = 10;
    int contextsPerThread = 100;
    int issuesPerContext = 10;

    Stopwatch stopwatch = Stopwatch.createStarted();

    ConcurrentHashSet<Exception> exceptions = new ConcurrentHashSet<>();
    ForkJoinPool forkJoinPool = new ForkJoinPool(threadCount);

    for (int i = 0; i < threadCount; i++) {
      int threadId = i;
      forkJoinPool.submit(() -> {
        try {
          runLoadTestThread(repository, threadId, contextsPerThread, issuesPerContext, useBatching);
        } catch (Exception ex) {
          exceptions.add(ex);
        }
      });
    }

    forkJoinPool.shutdown();
    assertThat(forkJoinPool.awaitTermination(30, TimeUnit.MINUTES)).isTrue();

    if (!exceptions.isEmpty()) {
      throw exceptions.stream().findFirst().get();
    }

    int totalIssues = threadCount * contextsPerThread * issuesPerContext;
    System.out.printf("Created %d issues in %d ms. Speed: %d issues/second%n", totalIssues,
                      stopwatch.elapsed(TimeUnit.MILLISECONDS), totalIssues / stopwatch.elapsed(TimeUnit.SECONDS));
  }

  private void runLoadTestThread(MySqlMultiContextIssueRepository repository, int threadNumber, int contextsPerThread,
      int issuesPerContext, boolean useBatching) {
    Random random = new Random(threadNumber);
    try {
      for (int i = 0; i < contextsPerThread; i++) {
        String contextId = "load-test-" + testId + "-thread-" + threadNumber + "-context-" + i;
        List<Issue> issues = new ArrayList<>();
        for (int j = 0; j < issuesPerContext; j++) {
          Issue issue = getLargeTestIssue("load-test-1-" + random.nextInt(), "code-" + random.nextInt());
          issues.add(issue);
          if (!useBatching) {
            repository.put(contextId, issue);
          }
        }
        if (useBatching) {
          repository.put(contextId, issues);
        }
        List<Issue> retrievedIssues = repository.getAll(contextId);
        assertThat(retrievedIssues).usingRecursiveComparison().isEqualTo(issues);
      }
    } catch (TroubleshooterException e) {
      throw new RuntimeException(e);
    }
  }

  private Issue getTestIssue(String summary, String code) {
    return Issue.builder().summary(summary).code(code)
        .time(ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC)).severity(IssueSeverity.ERROR)
        .details("test details for " + summary).build();
  }

  private Issue getLargeTestIssue(String summary, String code) {
    HashMap<String, String> properties = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      properties.put("test.property" + i, RandomStringUtils.random(100));
    }

    Issue.IssueBuilder issue = Issue.builder();
    issue.summary(summary);
    issue.code(code);
    issue.time(ZonedDateTime.now().withNano(0).withZoneSameInstant(ZoneOffset.UTC));
    issue.severity(IssueSeverity.ERROR);
    issue.details(RandomStringUtils.random(3000));
    issue.sourceClass(RandomStringUtils.random(100));
    issue.exceptionClass(RandomStringUtils.random(100));
    issue.properties(properties);

    return issue.build();
  }
}