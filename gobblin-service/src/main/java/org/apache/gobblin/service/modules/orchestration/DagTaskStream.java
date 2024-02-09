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

package org.apache.gobblin.service.modules.orchestration;

import java.util.Iterator;


/**
 * Holds a stream of {@link DagTask} that needs to be processed by the {@link DagManager}.
 * It provides an implementation for {@link DagManagement} defines the rules for a flow and job.
 * Implements {@link Iterator} to provide the next {@link DagTask} if available to {@link DagManager}
 */
public class DagTaskStream implements Iterator<DagTask>, DagManagement {
  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public DagTask next() {
    return null;
  }

  @Override
  public void launchFlow() {

  }

  @Override
  public void resumeFlow() {

  }

  @Override
  public void killFlow() {

  }

  @Override
  public void enforceFlowCompletionDeadline() {

  }

  @Override
  public void enforceJobStartDeadline() {

  }
}
