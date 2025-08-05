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

import java.util.Optional;

import lombok.Getter;
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

  public FileSizePolicy(State state, TaskLevelPolicy.Type type) {
    super(state, type);
  }

  @Override
  public Result executePolicy() {
    TransferBytes transferBytes = getBytesReadAndWritten(this.state).orElse(null);
    if (transferBytes == null) {
      return Result.FAILED;
    }
    Long bytesRead = transferBytes.getBytesRead();
    Long bytesWritten = transferBytes.getBytesWritten();

    Long sizeDifference = Math.abs(bytesRead - bytesWritten);

    if (sizeDifference == 0) {
      return Result.PASSED;
    }

    log.warn("File size check failed - bytes read: {}, bytes written: {}, difference: {}", bytesRead, bytesWritten,
        sizeDifference);
    return Result.FAILED;
  }

  @Override
  public String toString() {
    TransferBytes transferBytes = getBytesReadAndWritten(this.state).orElse(null);
    if (transferBytes != null) {
      return String.format("FileSizePolicy [bytesRead=%s, bytesWritten=%s]", transferBytes.getBytesRead(),
          transferBytes.getBytesWritten());
    } else {
      return "FileSizePolicy [bytesRead=null, bytesWritten=null]";
    }
  }

  /**
   * Helper class to hold transfer bytes information
   */
  @Getter
  private static class TransferBytes {
    final long bytesRead;
    final long bytesWritten;

    TransferBytes(long bytesRead, long bytesWritten) {
      this.bytesRead = bytesRead;
      this.bytesWritten = bytesWritten;
    }
  }

  /**
   * Extracts bytesRead and bytesWritten from the given state.
   * Returns Empty Optional if parsing fails.
   */
  private Optional<TransferBytes> getBytesReadAndWritten(State state) {
    String bytesReadString = state.getProp(BYTES_READ_KEY);
    String bytesWrittenString = state.getProp(BYTES_WRITTEN_KEY);
    if (bytesReadString == null || bytesWrittenString == null) {
      log.error("Missing value(s): bytesReadStr={}, bytesWrittenStr={}", bytesReadString, bytesWrittenString);
      return Optional.empty();
    }
    try {
      long bytesRead = Long.parseLong(bytesReadString);
      long bytesWritten = Long.parseLong(bytesWrittenString);
      return Optional.of(new TransferBytes(bytesRead, bytesWritten));
    } catch (NumberFormatException e) {
      log.error("Invalid number format for bytesRead or bytesWritten: bytesRead='{}', bytesWritten='{}'",
          bytesReadString, bytesWrittenString, e);
      return Optional.empty();
    }
  }
}
