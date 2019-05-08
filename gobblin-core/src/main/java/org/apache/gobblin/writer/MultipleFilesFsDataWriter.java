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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.Getter;

import static java.util.stream.Collectors.toList;


/**
 * An implementation of {@link DataWriter} that writes to multiple files if certain number
 * of records per file is reached.
 *
 * @author tilakpatidar@gmail.com
 */
public abstract class MultipleFilesFsDataWriter<D> extends FsDataWriter<D> {
  public static final Long WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT = 10000L;
  private static final Logger LOG = LoggerFactory.getLogger(MultipleFilesFsDataWriter.class);
  private final AtomicLong recordsWritten = new AtomicLong(0);
  private final AtomicLong recordsWrittenInCurrentWriter = new AtomicLong(0);
  private final AtomicLong currentWriterNumber = new AtomicLong(0);
  private final MultipleFilesFsDataWriterBuilder<?, D> builder;
  @Getter
  public Path currentFilePath;
  private Long recordsWrittenThreshold;
  private FileFormatWriter<D> currentWriter;

  public MultipleFilesFsDataWriter(MultipleFilesFsDataWriterBuilder<?, D> builder, State properties)
      throws IOException {
    super(builder, properties);
    this.recordsWrittenThreshold = properties.getPropAsLong(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_RECORDS_PER_FILE_THRESHOLD, this.numBranches, this.branchId),
        WRITER_RECORDS_PER_FILE_THRESHOLD_DEFAULT);
    this.builder = builder;
    this.currentWriter = this.getNewWriter();
  }

  @Override
  public void write(D record)
      throws IOException {
    if (this.recordsWrittenInCurrentWriter.incrementAndGet() == this.recordsWrittenThreshold) {
      LOG.debug("Record threshold {} reached", this.recordsWrittenThreshold.toString());
      this.closeCurrentWriter(this.currentWriter);
      this.currentWriter = this.getNewWriter();
      LOG.debug("Current writer number {}", this.currentWriterNumber.toString());
    }
    this.writeWithCurrentWriter(this.currentWriter, record);
    this.recordsWritten.incrementAndGet();
    LOG.debug("Total records written {} and records written {} in current writer", this.recordsWritten.toString(),
        this.recordsWrittenInCurrentWriter.toString());
  }

  @Override
  protected void setStagingFileGroup()
      throws IOException {
    for (WrittenFile writtenFile : writtenFiles()) {

      Preconditions.checkArgument(this.fs.exists(writtenFile.getStagingFile()),
          String.format("Staging output file %s does not exist", writtenFile.getStagingFile()));
      if (this.group.isPresent()) {
        HadoopUtils.setGroup(this.fs, writtenFile.getStagingFile(), this.group.get());
      }
    }
  }

  @Override
  public void commit()
      throws IOException {
    this.closer.close();

    this.setStagingFileGroup();

    Long bytesWritten = 0L;
    for (WrittenFile writtenFile : writtenFiles()) {
      if (!this.fs.exists(writtenFile.getStagingFile())) {
        throw new IOException(String.format("File %s does not exist", writtenFile.getStagingFile()));
      }
      FileStatus stagingFileStatus = this.fs.getFileStatus(writtenFile.getStagingFile());

      // Double check permission of staging file
      if (!stagingFileStatus.getPermission().equals(this.filePermission)) {
        this.fs.setPermission(writtenFile.getStagingFile(), this.filePermission);
      }
      bytesWritten += stagingFileStatus.getLen();
    }

    this.bytesWritten = Optional.of(bytesWritten);
    for (WrittenFile writtenFile : writtenFiles()) {
      LOG.info(String.format("Moving data from %s to %s", writtenFile.getStagingFile(), writtenFile.getOutputFile()));
      // For the same reason as deleting the staging file if it already exists, deleting
      // the output file if it already exists prevents task retry from being blocked.
      if (this.fs.exists(writtenFile.getOutputFile())) {
        LOG.warn(String.format("Task output file %s already exists", writtenFile.getOutputFile()));
        HadoopUtils.deletePath(this.fs, writtenFile.getOutputFile(), false);
      }
      HadoopUtils.renamePath(this.fs, writtenFile.getStagingFile(), writtenFile.getOutputFile());
    }
  }

  @Override
  public void cleanup()
      throws IOException {
    for (WrittenFile writtenFile : writtenFiles()) {
      // Delete the staging file
      if (this.fs.exists(writtenFile.getStagingFile())) {
        HadoopUtils.deletePath(this.fs, writtenFile.getStagingFile(), false);
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    this.closeCurrentWriter(this.currentWriter);
    super.close();
  }

  @Override
  public long recordsWritten() {
    return this.recordsWritten.get();
  }

  public abstract void writeWithCurrentWriter(FileFormatWriter<D> writer, D record)
      throws IOException;

  public abstract void closeCurrentWriter(FileFormatWriter<D> writer)
      throws IOException;

  private List<WrittenFile> writtenFiles() {
    return LongStream.range(1, this.currentWriterNumber.get()).mapToObj(WrittenFile::new).collect(toList());
  }

  private FileFormatWriter<D> getNewWriter()
      throws IOException {
    this.currentWriterNumber.incrementAndGet();
    this.recordsWrittenInCurrentWriter.set(0);
    Path stagingFile = getWriterStagingFileName(this.currentWriterNumber.get());
    this.currentFilePath = stagingFile;
    LOG.info("Created new writer for staging file {}", stagingFile.toString());
    return this.builder.getNewWriter((int) this.blockSize, stagingFile);
  }

  private Path generateNewFileName(Path file, long writerNumber) {
    String filePath = file.toString();
    String path = FilenameUtils.getFullPath(filePath);
    String baseName = FilenameUtils.getBaseName(filePath);
    String extension = FilenameUtils.getExtension(filePath);
    String fullExtension = extension.isEmpty() ? "" : "." + extension;
    String newFilePath = path + baseName + "_" + writerNumber + fullExtension;
    return new Path(newFilePath);
  }

  private Path getWriterStagingFileName(long writerNumber) {
    return generateNewFileName(this.stagingFile, writerNumber);
  }

  private Path getWriterOutputFileName(long writerNumber) {
    return generateNewFileName(this.outputFile, writerNumber);
  }

  @Getter
  private class WrittenFile {
    private final Path stagingFile;
    private final Path outputFile;

    WrittenFile(long writerNumber) {
      this.stagingFile = getWriterStagingFileName(writerNumber);
      this.outputFile = getWriterOutputFileName(writerNumber);
    }
  }
}