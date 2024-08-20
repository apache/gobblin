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

package org.apache.gobblin.service.modules.orchestration.task;

import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.LeaseAttemptStatus;


/**
 * A {@link DagTask} responsible for killing jobs if they have not started in
 * {@link org.apache.gobblin.service.ServiceConfigKeys#JOB_START_SLA_TIME}.
 */

public class EnforceJobStartDeadlineDagTask extends DagTask {
  public EnforceJobStartDeadlineDagTask(DagActionStore.DagAction dagAction, LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus,
      DagManagementStateStore dagManagementStateStore, DagProcessingEngineMetrics dagProcEngineMetrics) {
    super(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
  }

  public <T> T host(DagTaskVisitor<T> visitor) {
    return visitor.meet(this);
  }
}
