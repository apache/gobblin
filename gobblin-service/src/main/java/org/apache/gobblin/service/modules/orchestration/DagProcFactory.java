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
 * Factory for creating {@link DagProc} based on the visitor type for a given {@link DagTask}.
 */

@Alpha
public class DagProcFactory implements DagTaskVisitor<DagProc> {
  @Override
  public DagProc meet(LaunchDagTask launchDagTask) throws IOException, InstantiationException, IllegalAccessException {
    LaunchDagProc launchDagProc = new LaunchDagProc(launchDagTask.flowGroup, launchDagTask.flowName);
    return launchDagProc;
  }

  @Override
  public DagProc meet(KillDagTask killDagTask) throws IOException {
    KillDagProc killDagProc = new KillDagProc(killDagTask.killDagId);
    return killDagProc;
  }

  @Override
  public DagProc meet(ResumeDagTask resumeDagTask) throws IOException, InstantiationException, IllegalAccessException {
    ResumeDagProc resumeDagProc = new ResumeDagProc(resumeDagTask.resumeDagId);
    return resumeDagProc;
  }

  @Override
  public DagProc meet(AdvanceDagTask advanceDagTask) throws IOException {
    AdvanceDagProc advanceDagProc = new AdvanceDagProc();
    return advanceDagProc;
  }

  @Override
  public DagProc meet(CleanUpDagTask cleanUpDagTask) {
    CleanUpDagProc cleanUpDagProc = new CleanUpDagProc();
    return cleanUpDagProc;
  }
}

