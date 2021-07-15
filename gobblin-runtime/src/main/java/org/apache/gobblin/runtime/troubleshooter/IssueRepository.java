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
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;


/**
 * Collection of issues used by {@link AutomaticTroubleshooter}.
 *
 * The design assumes that there will typically be a few dozen issues per job, and the number of issues will never go
 * above a few hundreds. Since the issues are displayed to the user, there is no point in overwhelming them with too
 * many discovered problems.
 */
@ThreadSafe
public interface IssueRepository {
  List<Issue> getAll()
      throws TroubleshooterException;

  /**
   * Saves an issue to the repository, if it is not yet present.
   *
   * This method will ignore the issue, if another issues with the same code is already registered.
   */
  void put(Issue issue)
      throws TroubleshooterException;

  /**
   * @see #put(Issue)
   */
  void put(Collection<Issue> issues)
      throws TroubleshooterException;

  void remove(String issueCode)
      throws TroubleshooterException;

  void removeAll()
      throws TroubleshooterException;

  /**
   * Replaces all issues in the repository with a new collection
   */
  void replaceAll(Collection<Issue> issues)
      throws TroubleshooterException;
}
