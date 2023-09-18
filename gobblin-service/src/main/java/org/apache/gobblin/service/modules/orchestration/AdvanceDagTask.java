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

import java.io.IOException;

import org.apache.gobblin.annotation.Alpha;


/**
 * An implementation of {@link DagTask} that is responsible for advancing the dag to the next node based
 * on its current flow and job status. It is added to the {@link DagTaskStream} by the
 * {@link org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor} after it consumes the appropriate
 * {@link org.apache.gobblin.metrics.GobblinTrackingEvent} for the {@link org.apache.gobblin.service.modules.flowgraph.Dag}
 */

@Alpha
public class AdvanceDagTask extends DagTask {

  protected DagManager.DagId advanceDagId;

  @Override
  void initialize(Object state, long triggerTimeStamp) {

  }

  @Override
  AdvanceDagProc host(DagTaskVisitor visitor) throws IOException, InstantiationException, IllegalAccessException {
    return (AdvanceDagProc) visitor.meet(this);
  }
}
