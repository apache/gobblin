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

import org.apache.gobblin.service.modules.orchestration.task.AdvanceDagTask;
import org.apache.gobblin.service.modules.orchestration.task.CleanUpDagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ReloadDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.orchestration.task.RetryDagTask;


public interface DagTaskVisitor<D> {
  D meet(LaunchDagTask launchDagTask, DagProcessingEngine dagProcessingEngine);
  D meet(KillDagTask killDagTask, DagProcessingEngine dagProcessingEngine);
  D meet(ReloadDagTask killDagTask, DagProcessingEngine dagProcessingEngine);
  D meet(ResumeDagTask resumeDagTask, DagProcessingEngine dagProcessingEngine);
  D meet(RetryDagTask retryDagTask, DagProcessingEngine dagProcessingEngine);
  D meet(AdvanceDagTask advanceDagTask, DagProcessingEngine dagProcessingEngine);
  D meet(CleanUpDagTask cleanUpDagTask, DagProcessingEngine dagProcessingEngine);
}
