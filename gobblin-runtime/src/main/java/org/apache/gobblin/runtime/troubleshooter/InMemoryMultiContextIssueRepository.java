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

import org.apache.commons.collections4.map.LRUMap;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.gobblin.util.ConfigUtils;

/**
 * Stores issues from multiple jobs, flows or other contexts in memory.
 *
 * To limit the memory consumption, it will keep only the last {@link #MAX_CONTEXT_COUNT} contexts,
 * and older ones will be discarded.
 * */
@Singleton
public class InMemoryMultiContextIssueRepository implements MultiContextIssueRepository {
  public static final int DEFAULT_MAX_CONTEXT_COUNT = 100;

  public static final String CONFIG_PREFIX = "gobblin.troubleshooter.inMemoryIssueRepository.";
  public static final String MAX_CONTEXT_COUNT = CONFIG_PREFIX + "maxContextCount";
  public static final String MAX_ISSUE_PER_CONTEXT = CONFIG_PREFIX + "maxIssuesPerContext";

  private final LRUMap<String, InMemoryIssueRepository> contextIssues;
  private final int maxIssuesPerContext;

  public InMemoryMultiContextIssueRepository() {
    this(ConfigFactory.empty());
  }

  @Inject
  public InMemoryMultiContextIssueRepository(Config config) {
    this(ConfigUtils.getInt(config, MAX_CONTEXT_COUNT, DEFAULT_MAX_CONTEXT_COUNT),
         ConfigUtils.getInt(config, MAX_ISSUE_PER_CONTEXT, InMemoryIssueRepository.DEFAULT_MAX_SIZE));
  }

  public InMemoryMultiContextIssueRepository(int maxContextCount, int maxIssuesPerContext) {
    contextIssues = new LRUMap<>(maxContextCount);
    this.maxIssuesPerContext = maxIssuesPerContext;
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

    InMemoryIssueRepository issueRepository =
        contextIssues.computeIfAbsent(contextId, s -> new InMemoryIssueRepository(maxIssuesPerContext));

    issueRepository.put(issue);
  }

  @Override
  public synchronized void remove(String contextId, String issueCode)
      throws TroubleshooterException {

    InMemoryIssueRepository issueRepository = contextIssues.getOrDefault(contextId, null);

    if (issueRepository != null) {
      issueRepository.remove(issueCode);
    }
  }
}
