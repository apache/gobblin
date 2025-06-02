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

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy;

/**
 * A task-level policy that checks if the bytes read matches the bytes written for a file copy operation.
 */
@Slf4j
public class FileSizePolicy extends TaskLevelPolicy {

  public static final String COPY_PREFIX = "gobblin.copy";
  public static final String BYTES_READ_KEY = COPY_PREFIX + ".bytesRead";
  public static final String BYTES_WRITTEN_KEY = COPY_PREFIX + ".bytesWritten";

  private final Long bytesRead;
  private final Long bytesWritten;

  public FileSizePolicy(State state, TaskLevelPolicy.Type type) {
    super(state, type);
    String bytesReadString = state.getProp(BYTES_READ_KEY);
    String bytesWrittenString = state.getProp(BYTES_WRITTEN_KEY);
    this.bytesRead = bytesReadString == null ? null : Long.parseLong(bytesReadString);
    this.bytesWritten = bytesWrittenString == null ? null : Long.parseLong(bytesWrittenString);
  }

  @Override
  public Result executePolicy() {
    if(this.bytesRead == null || this.bytesWritten == null) {
      log.error("No bytes read or bytes written for this request");
      return Result.FAILED;
    }
    double sizeDifference = Math.abs(this.bytesRead - this.bytesWritten);

    if (sizeDifference == 0) {
      return Result.PASSED;
    }

    log.warn("File size check failed - bytes read: {}, bytes written: {}, difference: {}",
        this.bytesRead, this.bytesWritten, sizeDifference);
    return Result.FAILED;
  }

  @Override
  public String toString() {
    return String.format("FileSizePolicy [bytesRead=%s, bytesWritten=%s]",
        this.bytesRead, this.bytesWritten);
  }

}
