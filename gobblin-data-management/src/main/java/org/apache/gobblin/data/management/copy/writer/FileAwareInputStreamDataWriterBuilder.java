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
package org.apache.gobblin.data.management.copy.writer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;


/**
 * A {@link DataWriterBuilder} for {@link FileAwareInputStreamDataWriter}
 */
public class FileAwareInputStreamDataWriterBuilder extends DataWriterBuilder<String, FileAwareInputStream> {
  @Override
  public final DataWriter<FileAwareInputStream> build() throws IOException {
    setJobSpecificOutputPaths(this.destination.getProperties());
    // Each writer/mapper gets its own task-staging directory
    this.destination.getProperties().setProp(ConfigurationKeys.WRITER_FILE_PATH, this.writerId);
    return buildWriter();
  }

  protected DataWriter<FileAwareInputStream> buildWriter() throws IOException {
    return new FileAwareInputStreamDataWriter(this.destination.getProperties(), this.branches, this.branch, this.writerAttemptId);
  }

  /**
   * Each job gets its own task-staging and task-output directory. Update the staging and output directories to
   * contain job_id. This is to make sure uncleaned data from previous execution does not corrupt final published data
   * produced by this execution.
   */
  public synchronized static void setJobSpecificOutputPaths(State state) {

    // Other tasks may have set this already
    if (!StringUtils.containsIgnoreCase(state.getProp(ConfigurationKeys.WRITER_STAGING_DIR),
        state.getProp(ConfigurationKeys.JOB_ID_KEY))) {

      state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(state.getProp(ConfigurationKeys.WRITER_STAGING_DIR),
          state.getProp(ConfigurationKeys.JOB_ID_KEY)));
      state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
          state.getProp(ConfigurationKeys.JOB_ID_KEY)));

    }

  }
}
