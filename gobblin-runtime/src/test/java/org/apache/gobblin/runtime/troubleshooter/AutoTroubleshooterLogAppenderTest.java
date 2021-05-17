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

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.ThrowableWithErrorCode;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


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

    assertEquals(issue.getSeverity(), IssueSeverity.WARN);
    assertEquals(issue.getSummary(), "test");
    assertEquals(issue.getSourceClass(), getClass().getName());
        assertTrue(issue.getTime().isAfter(ZonedDateTime.now().minus(1, ChronoUnit.MINUTES)));
    assertTrue(issue.getTime().isBefore(ZonedDateTime.now().plus(1, ChronoUnit.MINUTES)));
    assertTrue(issue.getCode().length() > 1);

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

    assertEquals(issue.getSeverity(), IssueSeverity.ERROR);
    assertTrue(issue.getSummary().contains("test message"));
    assertTrue(issue.getSummary().contains("test exception"));
    assertTrue(issue.getCode().length() > 1);
    assertTrue(issue.getDetails().contains("IOException"));
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

    assertEquals(issue.getSeverity(), IssueSeverity.ERROR);
    assertEquals(issue.getCode(), "TestCode");
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