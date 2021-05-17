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

import java.time.ZonedDateTime;
import java.util.Map;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


/**
 * Issue describes a specific unique problem in the job or application.
 *
 * Issue can be generated from log entries, health checks, and other places.
 *
 * @see AutomaticTroubleshooter
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class Issue {
  public static final int MAX_ISSUE_CODE_LENGTH = 100;
  public static final int MAX_CLASSNAME_LENGTH = 1000;

  private final ZonedDateTime time;
  private final IssueSeverity severity;

  /**
   * Unique code that identifies a specific problem.
   *
   * It can be used for making programmatic decisions on how to handle and recover from this issue.
   *
   * The code length should be less than {@link Issue.MAX_ISSUE_CODE_LENGTH}
   * */
  private final String code;

  /**
   * Short, human-readable description of the issue.
   *
   * It should focus on what is the root cause of the problem, and what steps the user should do to resolve it.
   */
  private final String summary;

  /**
   * Human-readable issue details that can include exception stack trace and additional information about the problem.
   */
  private final String details;

  /**
   * Unique name of the component that produced the issue.
   *
   * This is a full name of the class that logged the error or generated the issue.
   *
   * The class name length should be less than {@link Issue.MAX_CLASSNAME_LENGTH}
   * */
  private final String sourceClass;

  /**
   * If the issue was generated from an exception, then a full exception class name should be stored here.
   *
   * The class name length should be less than {@link Issue.MAX_CLASSNAME_LENGTH}
   */
  private final String exceptionClass;

  /**
   * Additional machine-readable properties of the issue.
   *
   * Those can be used to forward information to the system that will analyze the issues across the platform.
   */
  private final Map<String, String> properties;
}
