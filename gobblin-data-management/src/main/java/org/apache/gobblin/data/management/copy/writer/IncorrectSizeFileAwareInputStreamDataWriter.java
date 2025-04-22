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

import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.policies.size.FileSizePolicy;
import org.apache.gobblin.writer.DataWriter;
import org.apache.hadoop.fs.Path;

/**
 * A {@link DataWriter} that extends {@link FileAwareInputStreamDataWriter} to intentionally report incorrect file sizes.
 * This is useful for testing data quality checks that verify file sizes.
 *
 * The writer actually writes the correct data to the destination, but reports incorrect sizes in the bytesWritten() method.
 * The size discrepancy can be configured through properties:
 * - gobblin.copy.incorrect.size.ratio: Ratio to multiply actual size by (default 1.0)
 * - gobblin.copy.incorrect.size.offset: Fixed offset to add to actual size (default 0)
 */
@Slf4j
public class IncorrectSizeFileAwareInputStreamDataWriter extends FileAwareInputStreamDataWriter {

  public static final String INCORRECT_SIZE_RATIO_KEY = "gobblin.copy.incorrect.size.ratio";
  public static final String INCORRECT_SIZE_OFFSET_KEY = "gobblin.copy.incorrect.size.offset";
  public static final String DESTINATION_FILE_SIZE_KEY = "gobblin.copy.destination.file.size";
  public static final double DEFAULT_INCORRECT_SIZE_RATIO = 0.9;
  public static final long DEFAULT_INCORRECT_SIZE_OFFSET = 0L;

  private final double sizeRatio;
  private final long sizeOffset;

  public IncorrectSizeFileAwareInputStreamDataWriter(State state, int numBranches, int branchId)
      throws IOException {
    this(state, numBranches, branchId, null);
  }

  public IncorrectSizeFileAwareInputStreamDataWriter(State state, int numBranches, int branchId, String writerAttemptId)
      throws IOException {
    super(state, numBranches, branchId, writerAttemptId);
    this.sizeRatio = state.getPropAsDouble(INCORRECT_SIZE_RATIO_KEY, DEFAULT_INCORRECT_SIZE_RATIO);
    this.sizeOffset = state.getPropAsLong(INCORRECT_SIZE_OFFSET_KEY, DEFAULT_INCORRECT_SIZE_OFFSET);
    log.info("Initialized IncorrectSizeFileAwareInputStreamDataWriter with ratio={}, offset={}",
        this.sizeRatio, this.sizeOffset);
  }

  @Override
  protected void writeImpl(InputStream inputStream, Path writeAt, CopyableFile copyableFile,
      FileAwareInputStream record) throws IOException {
    // First call parent's writeImpl to do the actual writing
    super.writeImpl(inputStream, writeAt, copyableFile, record);

    // After writing is complete, update the state with actual and incorrect sizes
    long actualDestSize = this.fs.getFileStatus(writeAt).getLen();
    long incorrectBytes = (long)(actualDestSize * this.sizeRatio) + this.sizeOffset;

    // Store both actual and incorrect sizes in state
    this.state.setProp(DESTINATION_FILE_SIZE_KEY, actualDestSize);
//    this.state.setProp("gobblin.copy.reported.file.size", incorrectBytes);
    this.state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, incorrectBytes);

    log.info("File size reporting: actual={}, reported={} (ratio={}, offset={})",
        actualDestSize, incorrectBytes, this.sizeRatio, this.sizeOffset);
  }

  @Override
  public long bytesWritten() throws IOException {
    long actualBytes = super.bytesWritten();
    return (long)(actualBytes * this.sizeRatio) + this.sizeOffset;
  }
}
