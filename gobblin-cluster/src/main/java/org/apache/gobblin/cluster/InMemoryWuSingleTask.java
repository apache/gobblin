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
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

/**
 * Instead of deserializing {@link JobState} and {@link WorkUnit} from filesystem, create them in memory.
 * Uses {@link DummyDataWriter} so that the execution of a Task goes through.
 *
 * This extension class added a declared dummyWriterBuilder so that the task execution will go through.
 * This class is primarily designed for testing purpose.
 */
public class InMemoryWuSingleTask extends SingleTask {
  public InMemoryWuSingleTask(String jobId, Path workUnitFilePath, Path jobStateFilePath, FileSystem fs,
      TaskAttemptBuilder taskAttemptBuilder, StateStores stateStores, Config dynamicConfig) {
    super(jobId, workUnitFilePath, jobStateFilePath, fs, taskAttemptBuilder, stateStores, dynamicConfig);
  }

  @Override
  protected List<WorkUnit> getWorkUnits()
      throws IOException {
    WorkUnit workUnit = new WorkUnit();
    workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, "randomTask");
    workUnit.setProp("source.class", "org.apache.gobblin.cluster.DummySource");
    // Missing this line leads to failure in precondition check of avro writer.
    workUnit.setProp(ConfigurationKeys.WRITER_BUILDER_CLASS, DummyDataWriterBuilder.class.getName());
    return Lists.newArrayList(workUnit);
  }

  @Override
  protected JobState getJobState()
      throws IOException {
    JobState jobState = new JobState("randomJobName", "randomJobId");
    return jobState;
  }

  public static class DummyDataWriterBuilder extends DataWriterBuilder<String, Integer> {

    @Override
    public DataWriter<Integer> build() throws IOException {
      return new DummyDataWriter();
    }
  }

  private static class DummyDataWriter implements DataWriter<Integer> {

    @Override
    public void write(Integer record) throws IOException {
      // Nothing to do
    }

    @Override
    public void commit() throws IOException {
      // Nothing to do
    }

    @Override
    public void cleanup() throws IOException {
      // Nothing to do
    }

    @Override
    public long recordsWritten() {
      return 0;
    }

    @Override
    public long bytesWritten() throws IOException {
      return  0;
    }

    @Override
    public void close() throws IOException {
      // Nothing to do
    }
  }
}
