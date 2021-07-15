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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;


/**
 * In-memory repository of troubleshooter issues.
 *
 * This repository has a maximum size to avoid running out of memory when application code produces
 * unexpectedly large number of issues. When the repository is full, new issues will be ignored.
 *
 * When multiple issues with the same issue code are put into the repository, only the first one
 * will be stored. Others will be treated as duplicates, and will be discarded.
 *
 * */
@Singleton
@Slf4j
public class InMemoryIssueRepository implements IssueRepository {

  public static final int DEFAULT_MAX_SIZE = 100;

  private final LinkedHashMap<String, Issue> issues = new LinkedHashMap<>();
  private final int maxSize;

  private boolean reportedOverflow = false;

  @Inject
  public InMemoryIssueRepository() {
    this(DEFAULT_MAX_SIZE);
  }

  public InMemoryIssueRepository(int maxSize) {
    this.maxSize = maxSize;
  }

  @Override
  public synchronized List<Issue> getAll()
      throws TroubleshooterException {

    return new ArrayList<>(issues.values());
  }

  @Override
  public synchronized void put(Issue issue)
      throws TroubleshooterException {

    if (issues.size() >= maxSize) {
      if (!reportedOverflow) {
        reportedOverflow = true;
        log.warn("In-memory issue repository has {} elements and is now full. New issues will be ignored.",
                 issues.size());
      }
      return;
    }

    if (!issues.containsKey(issue.getCode())) {
      issues.put(issue.getCode(), issue);
    }
  }

  @Override
  public synchronized void put(Collection<Issue> issues)
      throws TroubleshooterException {
    for (Issue issue : issues) {
      put(issue);
    }
  }

  @Override
  public synchronized void remove(String code)
      throws TroubleshooterException {
    issues.remove(code);
  }

  @Override
  public synchronized void removeAll()
      throws TroubleshooterException {
    issues.clear();
  }

  @Override
  public synchronized void replaceAll(Collection<Issue> issues)
      throws TroubleshooterException {
    removeAll();
    put(issues);
  }
}
