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

import com.google.common.util.concurrent.Service;


/**
 * Stores issues from multiple jobs, flows and other contexts.
 *
 * For each context, there can only be one issue with a specific code.
 *
 * @see AutomaticTroubleshooter
 * */
public interface MultiContextIssueRepository extends Service {

  /**
   * Will return issues in the same order as they were put into the repository.
   * */
  List<Issue> getAll(String contextId)
      throws TroubleshooterException;

  void put(String contextId, Issue issue)
      throws TroubleshooterException;

  void put(String contextId, List<Issue> issues)
      throws TroubleshooterException;

  void remove(String contextId, String issueCode)
      throws TroubleshooterException;
}
