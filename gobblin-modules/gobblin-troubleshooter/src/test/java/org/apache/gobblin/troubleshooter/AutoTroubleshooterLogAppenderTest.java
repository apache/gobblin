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

package org.apache.gobblin.troubleshooter;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.ThrowableWithErrorCode;
import org.apache.gobblin.runtime.troubleshooter.InMemoryIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;

import static org.testng.Assert.assertEquals;


public class AutoTroubleshooterLogAppenderTest {

  private final static Logger log = LogManager.getLogger(AutoTroubleshooterLogAppenderTest.class);

  IssueRepository issueRepository;
  AutoTroubleshooterLogAppender appender;

  @BeforeMethod
  public void setUp() {
    issueRepository = new InMemoryIssueRepository(100);
    appender = new AutoTroubleshooterLogAppender(issueRepository);
  }

  @Test
  public void canLogWarning()
      throws Exception {

    appender.append(new LoggingEvent(log.getName(), log, System.currentTimeMillis(), Level.WARN, "test", null));

    Issue issue = issueRepository.getAll().get(0);

    Assert.assertEquals(issue.getSeverity(), IssueSeverity.WARN);
    Assert.assertEquals(issue.getSummary(), "test");
    Assert.assertEquals(issue.getSourceClass(), getClass().getName());
    Assert.assertTrue(issue.getTime().isAfter(ZonedDateTime.now().minus(1, ChronoUnit.MINUTES)));
    Assert.assertTrue(issue.getTime().isBefore(ZonedDateTime.now().plus(1, ChronoUnit.MINUTES)));
    Assert.assertTrue(issue.getCode().length() > 1);

    assertEquals(appender.getProcessedEventCount(), 1);
  }

  @Test
  public void canLogException()
      throws Exception {

    Exception exception;
    try {
      // Throwing exception to get a real stack trace in it
      throw new IOException("test exception");
    } catch (Exception e) {
      exception = e;
    }

    appender.append(
        new LoggingEvent(log.getName(), log, System.currentTimeMillis(), Level.ERROR, "test message", exception));

    Issue issue = issueRepository.getAll().get(0);

    Assert.assertEquals(issue.getSeverity(), IssueSeverity.ERROR);
    Assert.assertTrue(issue.getSummary().contains("test message"));
    Assert.assertTrue(issue.getSummary().contains("test exception"));
    Assert.assertTrue(issue.getCode().length() > 1);
    Assert.assertTrue(issue.getDetails().contains("IOException"));
  }

  @Test
  public void willGetSameErrorCodesForSameStackTraces()
      throws Exception {

    for (int i = 0; i < 5; i++) {
      Exception exception;
      try {
        // Throwing exception to get a real stack trace in it
        // Messages are intentionally different in every loop. We are checking that as all exceptions with
        // same stack trace will get the same event code

        try {
          throw new InvalidOperationException("test inner exception " + i);
        } catch (Exception inner) {
          throw new IOException("test outer exception " + i, inner);
        }
      } catch (Exception e) {
        exception = e;
      }

      appender.append(
          new LoggingEvent(log.getName(), log, System.currentTimeMillis(), Level.ERROR, "test message", exception));
    }

    List<Issue> issues = issueRepository.getAll();
    Assert.assertEquals(issues.size(), 1); // all issues should have the same error code and get deduplicated
  }

  @Test
  public void canLogExceptionWithSpecificErrorCode()
      throws Exception {

    Exception exception;
    try {
      throw new TestException("test exception", "TestCode");
    } catch (Exception e) {
      exception = e;
    }

    appender.append(
        new LoggingEvent(log.getName(), log, System.currentTimeMillis(), Level.ERROR, "test message", exception));

    Issue issue = issueRepository.getAll().get(0);

    Assert.assertEquals(issue.getSeverity(), IssueSeverity.ERROR);
    Assert.assertEquals(issue.getCode(), "TestCode");
  }

  private static class TestException extends Exception implements ThrowableWithErrorCode {
    String errorCode;

    public TestException(String message, String errorCode) {
      super(message);
      this.errorCode = errorCode;
    }

    @Override
    public String getErrorCode() {
      return errorCode;
    }
  }
}