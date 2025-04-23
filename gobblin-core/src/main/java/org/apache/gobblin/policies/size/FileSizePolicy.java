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

package org.apache.gobblin.policies.size;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy;

/**
 * A task-level policy that checks if the bytes read match the bytes written for a file copy operation.
 */
public class FileSizePolicy extends TaskLevelPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(FileSizePolicy.class);

  public static final String BYTES_READ_KEY = "gobblin.copy.bytesRead";
  public static final String BYTES_WRITTEN_KEY = "gobblin.copy.bytesWritten";

  private final long bytesRead;
  private final long bytesWritten;

  public FileSizePolicy(State state, TaskLevelPolicy.Type type) {
    super(state, type);
    this.bytesRead = state.getPropAsLong(BYTES_READ_KEY, 0);
    this.bytesWritten = state.getPropAsLong(BYTES_WRITTEN_KEY, 0);
  }

  @Override
  public Result executePolicy() {
    double sizeDifference = Math.abs(this.bytesRead - this.bytesWritten);

    if (sizeDifference == 0) {
      return Result.PASSED;
    }

    LOG.warn("File size check failed - bytes read: {}, bytes written: {}, difference: {}",
        this.bytesRead, this.bytesWritten, sizeDifference);
    return Result.FAILED;
  }

  @Override
  public String toString() {
    return String.format("FileSizePolicy [bytesRead=%s, bytesWritten=%s]",
        this.bytesRead, this.bytesWritten);
  }

}