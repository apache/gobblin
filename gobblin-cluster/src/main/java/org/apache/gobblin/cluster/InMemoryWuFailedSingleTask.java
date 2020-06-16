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

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.util.List;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;


/**
 * Instead of deserializing {@link JobState} and {@link WorkUnit} from filesystem, create them in memory.
 * This derived class will be failing due to missing the declaration of writerBuilder class thereby failing a Precondition
 * check in AvroWriterBuilder which is used by default.
 */
public class InMemoryWuFailedSingleTask extends SingleTask {
  public InMemoryWuFailedSingleTask(String jobId, Path workUnitFilePath, Path jobStateFilePath, FileSystem fs,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores, Config dynamicConfig) {
    super(jobId, workUnitFilePath, jobStateFilePath, fs, taskAttemptBuilder, stateStores, dynamicConfig);
  }

  @Override
  protected List<WorkUnit> getWorkUnits()
      throws IOException {
    // Create WorkUnit in memory.
    WorkUnit workUnit = new WorkUnit();
    workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, "randomTask");
    workUnit.setProp("source.class", "org.apache.gobblin.cluster.DummySource");
    return Lists.newArrayList(workUnit);
  }

  @Override
  protected JobState getJobState()
      throws IOException {
    JobState jobState = new JobState("randomJobName", "randomJobId");
    return jobState;
  }
}