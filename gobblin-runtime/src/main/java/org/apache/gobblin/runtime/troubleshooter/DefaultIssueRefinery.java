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

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableList;


/**
 * Provides basic issue sorting and cleanup.
 *
 * Issue refining will be improved in future PRs
 */
public class DefaultIssueRefinery implements IssueRefinery {
  @Override
  public List<Issue> refine(ImmutableList<Issue> sourceIssues) {
    LinkedList<Issue> issues = new LinkedList<>(sourceIssues);

    issues.sort(Comparator.comparing(Issue::getSeverity).reversed().thenComparing(Issue::getTime));

    // Kafka warnings usually don't impact the job outcome, so we are hiding them from the user
    issues.removeIf(i -> i.getSeverity().isEqualOrLowerThan(IssueSeverity.WARN) && StringUtils
        .containsIgnoreCase(i.getSourceClass(), "com.linkedin.kafka"));
    issues.removeIf(i -> i.getSeverity().isEqualOrLowerThan(IssueSeverity.WARN) && StringUtils
        .containsIgnoreCase(i.getSourceClass(), "org.apache.kafka"));

    // Metrics-related issues usually don't impact the outcome of the job
    moveToBottom(issues, i -> StringUtils.containsIgnoreCase(i.getSourceClass(), "org.apache.gobblin.metrics"));

    return issues;
  }

  private void moveToBottom(LinkedList<Issue> issues, java.util.function.Predicate<Issue> predicate) {
    Collection<Issue> movedIssues = issues.stream().filter(predicate).collect(Collectors.toList());
    issues.removeAll(movedIssues);
    issues.addAll(movedIssues);
  }
}
