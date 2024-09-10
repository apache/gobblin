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
package org.apache.gobblin.temporal.ddm.activity;

import java.util.Set;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import org.apache.gobblin.temporal.ddm.work.DirDeletionResult;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;


/** Activity for deleting up a list of temporary work directories */
@ActivityInterface
public interface DeleteWorkDirsActivity {
  /**
   * Clean the list of resources specified in the input
   * TODO: Generalize the input to support multiple platforms outside of just HDFS
   */
  @ActivityMethod
  DirDeletionResult delete(WUProcessingSpec workSpec, EventSubmitterContext eventSubmitterContext, Set<String> workDirPaths);
}

