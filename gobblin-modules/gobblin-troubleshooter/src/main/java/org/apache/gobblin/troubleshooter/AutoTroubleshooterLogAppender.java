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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.ThrowableWithErrorCode;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;


/**
 * Collects messages from log4j and converts them into issues that are used in {@link AutomaticTroubleshooter}.
 */
@Slf4j
@ThreadSafe
public class AutoTroubleshooterLogAppender extends AppenderSkeleton {
  private static final int AUTO_GENERATED_HASH_LENGTH = 6;
  private static final String AUTO_GENERATED_HASH_PREFIX = "T";

  private final IssueRepository repository;

  private final AtomicBoolean reportedRepositoryError = new AtomicBoolean(false);
  private final AtomicInteger processedEventCount = new AtomicInteger();

  public AutoTroubleshooterLogAppender(IssueRepository issueRepository) {
    this.repository = Objects.requireNonNull(issueRepository);
  }

  private static String getHash(String text) {
    return AUTO_GENERATED_HASH_PREFIX + DigestUtils.sha256Hex(text).substring(0, AUTO_GENERATED_HASH_LENGTH)
        .toUpperCase();
  }

  public int getProcessedEventCount() {
    return processedEventCount.get();
  }

  @Override
  protected void append(LoggingEvent event) {
    processedEventCount.incrementAndGet();

    Issue issue = convertToIssue(event);

    try {
      repository.put(issue);
    } catch (TroubleshooterException e) {
      if (reportedRepositoryError.compareAndSet(false, true)) {
        log.warn("Failed to save the issue to the repository", e);
      }
    }
  }

  private Issue convertToIssue(LoggingEvent event) {
    Issue.IssueBuilder issueBuilder =
        Issue.builder().time(ZonedDateTime.ofInstant(Instant.ofEpochMilli(event.getTimeStamp()), ZoneOffset.UTC))
            .severity(convert(event.getLevel())).code(getIssueCode(event)).sourceClass(event.getLoggerName());

    if (event.getThrowableInformation() != null) {
      Throwable throwable = event.getThrowableInformation().getThrowable();
      issueBuilder.details(ExceptionUtils.getStackTrace(throwable));

      String summarizedException =
          StringUtils.substringBefore(ExceptionUtils.getRootCauseMessage(throwable), System.lineSeparator());
      issueBuilder.summary(summarizedException + " | " + event.getRenderedMessage());
    } else {
      issueBuilder.summary(event.getRenderedMessage());
    }

    return issueBuilder.build();
  }

  private String getIssueCode(LoggingEvent event) {
    if (event.getThrowableInformation() != null) {
      return getIssueCode(event.getThrowableInformation().getThrowable());
    }

    LocationInfo locationInformation = event.getLocationInformation();

    if (locationInformation.fullInfo != null) {
      String locationInCode = locationInformation.getClassName() + locationInformation.getLineNumber();
      return getHash(locationInCode);
    } else {
      return getHash(event.getLoggerName() + event.getMessage().toString());
    }
  }

  private String getIssueCode(Throwable throwable) {
    if (throwable instanceof ThrowableWithErrorCode) {
      return ((ThrowableWithErrorCode) throwable).getErrorCode();
    }

    /*
     * Ideally, each exception should have a unique machine-readable error code. Then we can easily group them together
     * and remove duplicates. However, it’s not feasible to add error codes to the large legacy codebase overnight, so
     * we generate them automatically.
     *
     * Good error codes should identify one specific problem, so they don’t always map to exception types.
     * For example “FileNotFoundException” can mean that a user's file is not found, or some config file that job
     * expects was not found, or credentials file is missing, and so on.
     *
     * Exception messages can have path names, job ids, and other unpredictable variable parts.
     * So, even if the problem is exactly the same, the messages could be different.
     *
     * We pick an option to generate an error code as a hash of exception type and stack trace. This will produce
     * a unique error code of the situation. However, when a codebase is refactored, stacktraces can change.
     * As a result, such automatic error codes can be different between application versions.
     * This should be fine within a single job, but it can affect system-wide reports that process data from
     * multiple application versions.
     * */

    return getHash(getStackTraceWithoutExceptionMessage(throwable));
  }

  private String getStackTraceWithoutExceptionMessage(Throwable throwable) {
    StrBuilder sb = new StrBuilder();

    for (Throwable currentThrowable : ExceptionUtils.getThrowableList(throwable)) {
      sb.appendln(currentThrowable.getClass().getName());
      for (StackTraceElement stackTraceElement : currentThrowable.getStackTrace()) {
        sb.appendln(stackTraceElement);
      }
      sb.appendln("---");
    }

    return sb.toString();
  }

  private IssueSeverity convert(Level level) {
    if (level == Level.TRACE || level == Level.DEBUG) {
      return IssueSeverity.DEBUG;
    } else if (level == Level.INFO) {
      return IssueSeverity.INFO;
    } else if (level == Level.WARN) {
      return IssueSeverity.WARN;
    } else if (level == Level.ERROR) {
      return IssueSeverity.ERROR;
    } else if (level == Level.FATAL) {
      return IssueSeverity.FATAL;
    }

    return IssueSeverity.DEBUG;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
