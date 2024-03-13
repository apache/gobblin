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

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.proc.LaunchDagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;


/**
 * {@link DagTaskVisitor} for transforming a specific {@link DagTask} derived class to its companion {@link DagProc} derived class.
 * Each {@link DagTask} needs it own {@link DagProcFactory#meet} method overload to create {@link DagProc} that is
 * supposed to process that {@link DagTask}.
 */

@Alpha
@Singleton
public class DagProcFactory implements DagTaskVisitor<DagProc> {

  private final FlowCompilationValidationHelper flowCompilationValidationHelper;

  @Inject
  public DagProcFactory(FlowCompilationValidationHelper flowCompilationValidationHelper) {
    this.flowCompilationValidationHelper = flowCompilationValidationHelper;
  }

  @Override
  public LaunchDagProc meet(LaunchDagTask launchDagTask) {
    return new LaunchDagProc(launchDagTask, this.flowCompilationValidationHelper);
  }
  //todo - overload meet method for other dag tasks
}

