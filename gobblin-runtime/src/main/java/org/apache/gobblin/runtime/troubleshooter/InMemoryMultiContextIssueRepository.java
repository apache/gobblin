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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections4.map.LRUMap;

import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.gobblin.service.ServiceConfigKeys;


/**
 * Stores issues from multiple jobs, flows or other contexts in memory.
 *
 * To limit the memory consumption, it will keep only the last {@link Configuration#maxContextCount} contexts,
 * and older ones will be discarded.
 * */
@Singleton
public class InMemoryMultiContextIssueRepository extends AbstractIdleService implements MultiContextIssueRepository {
  private final LRUMap<String, InMemoryIssueRepository> contextIssues;
  private final Configuration configuration;

  public InMemoryMultiContextIssueRepository() {
    this(Configuration.builder().build());
  }

  @Inject
  public InMemoryMultiContextIssueRepository(Configuration configuration) {
    this.configuration = Objects.requireNonNull(configuration);
    contextIssues = new LRUMap<>(configuration.getMaxContextCount());
  }

  @Override
  public synchronized List<Issue> getAll(String contextId)
      throws TroubleshooterException {

    InMemoryIssueRepository issueRepository = contextIssues.getOrDefault(contextId, null);

    if (issueRepository != null) {
      return issueRepository.getAll();
    }

    return Collections.emptyList();
  }

  @Override
  public synchronized void put(String contextId, Issue issue)
      throws TroubleshooterException {
    InMemoryIssueRepository issueRepository = contextIssues
        .computeIfAbsent(contextId, s -> new InMemoryIssueRepository(configuration.getMaxIssuesPerContext()));

    issueRepository.put(issue);
  }

  @Override
  public synchronized void put(String contextId, List<Issue> issues)
      throws TroubleshooterException {

    InMemoryIssueRepository issueRepository = contextIssues
        .computeIfAbsent(contextId, s -> new InMemoryIssueRepository(configuration.getMaxIssuesPerContext()));

    issueRepository.put(issues);
  }

  @Override
  public synchronized void remove(String contextId, String issueCode)
      throws TroubleshooterException {

    InMemoryIssueRepository issueRepository = contextIssues.getOrDefault(contextId, null);

    if (issueRepository != null) {
      issueRepository.remove(issueCode);
    }
  }

  @Override
  protected void startUp()
      throws Exception {
  }

  @Override
  protected void shutDown()
      throws Exception {
  }

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class Configuration {

    @Builder.Default
    private int maxContextCount = ServiceConfigKeys.DEFAULT_MEMORY_ISSUE_REPO_MAX_CONTEXT_COUNT;

    @Builder.Default
    private int maxIssuesPerContext = ServiceConfigKeys.DEFAULT_MEMORY_ISSUE_REPO_MAX_ISSUE_PER_CONTEXT;

    @Inject
    public Configuration(Config innerConfig) {
      this();
      if (innerConfig.hasPath(ServiceConfigKeys.MEMORY_ISSUE_REPO_MAX_CONTEXT_COUNT)) {
        maxContextCount = innerConfig.getInt(ServiceConfigKeys.MEMORY_ISSUE_REPO_MAX_CONTEXT_COUNT);
      }

      if (innerConfig.hasPath(ServiceConfigKeys.MEMORY_ISSUE_REPO_MAX_ISSUE_PER_CONTEXT)) {
        maxIssuesPerContext = innerConfig.getInt(ServiceConfigKeys.MEMORY_ISSUE_REPO_MAX_ISSUE_PER_CONTEXT);
      }
    }
  }
}
