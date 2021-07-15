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
import java.util.Collections;
import java.util.List;


public class NoopIssueRepository implements IssueRepository {
  @Override
  public List<Issue> getAll()
      throws TroubleshooterException {
    return Collections.emptyList();
  }

  @Override
  public void put(Issue issue)
      throws TroubleshooterException {

  }

  @Override
  public void put(Collection<Issue> issues)
      throws TroubleshooterException {

  }

  @Override
  public void remove(String issueCode)
      throws TroubleshooterException {

  }

  @Override
  public void removeAll()
      throws TroubleshooterException {

  }

  @Override
  public void replaceAll(Collection<Issue> issues)
      throws TroubleshooterException {

  }
}
