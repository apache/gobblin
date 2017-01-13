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

package gobblin.writer;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import gobblin.commit.SpeculativeAttemptAwareConstruct;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.FinalState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.JobConfigurationUtils;
import gobblin.util.WriterUtils;
import gobblin.util.recordcount.IngestionRecordCountProvider;


/**
 * An implementation of {@link DataWriter} does the work of setting the output/staging dir
 * and creating the FileSystem instance.
 *
 * @author akshay@nerdwallet.com
 */
public abstract class FsDataWriter<D> implements DataWriter<D>, FinalState, SpeculativeAttemptAwareConstruct {

  private static final Logger LOG = LoggerFactory.getLogger(FsDataWriter.class);

  public static final String WRITER_INCLUDE_RECORD_COUNT_IN_FILE_NAMES =
      ConfigurationKeys.WRITER_PREFIX + ".include.record.count.in.file.names";

  protected final State properties;
  protected final String id;
  protected final int numBranches;
  protected final int branchId;
  protected final String fileName;
  protected final FileSystem fs;
  protected final Path stagingFile;
  protected Path outputFile;
  protected final String allOutputFilesPropName;
  protected final boolean shouldIncludeRecordCountInFileName;
  protected final int bufferSize;
  protected final short replicationFactor;
  protected final long blockSize;
  protected final FsPermission filePermission;
  protected final FsPermission dirPermission;
  protected final Optional<String> group;
  protected final Closer closer = Closer.create();
  protected final Optional<String> writerAttemptIdOptional;
  protected Optional<Long> bytesWritten;

  public FsDataWriter(FsDataWriterBuilder<?, D> builder, State properties)
      throws IOException {
    this.properties = properties;
    this.id = builder.getWriterId();
    this.numBranches = builder.getBranches();
    this.branchId = builder.getBranch();
    this.fileName = builder.getFileName(properties);
    this.writerAttemptIdOptional = Optional.fromNullable(builder.getWriterAttemptId());

    Configuration conf = new Configuration();
    // Add all job configuration properties so they are picked up by Hadoop
    JobConfigurationUtils.putStateIntoConfiguration(properties, conf);
    this.fs = WriterUtils.getWriterFS(properties, this.numBranches, this.branchId);

    // Initialize staging/output directory
    Path writerStagingDir = this.writerAttemptIdOptional.isPresent() ? WriterUtils
        .getWriterStagingDir(properties, this.numBranches, this.branchId, this.writerAttemptIdOptional.get())
        : WriterUtils.getWriterStagingDir(properties, this.numBranches, this.branchId);
    this.stagingFile = new Path(writerStagingDir, this.fileName);

    this.outputFile =
        new Path(WriterUtils.getWriterOutputDir(properties, this.numBranches, this.branchId), this.fileName);
    this.allOutputFilesPropName = ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, this.numBranches, this.branchId);

    // Deleting the staging file if it already exists, which can happen if the
    // task failed and the staging file didn't get cleaned up for some reason.
    // Deleting the staging file prevents the task retry from being blocked.
    if (this.fs.exists(this.stagingFile)) {
      LOG.warn(String.format("Task staging file %s already exists, deleting it", this.stagingFile));
      HadoopUtils.deletePath(this.fs, this.stagingFile, false);
    }

    this.shouldIncludeRecordCountInFileName = properties.getPropAsBoolean(ForkOperatorUtils
        .getPropertyNameForBranch(WRITER_INCLUDE_RECORD_COUNT_IN_FILE_NAMES, this.numBranches, this.branchId), false);

    this.bufferSize = properties.getPropAsInt(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_BUFFER_SIZE, this.numBranches, this.branchId),
        ConfigurationKeys.DEFAULT_BUFFER_SIZE);

    this.replicationFactor = properties.getPropAsShort(ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_REPLICATION_FACTOR, this.numBranches, this.branchId),
        this.fs.getDefaultReplication(this.outputFile));

    this.blockSize = properties.getPropAsLong(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_BLOCK_SIZE, this.numBranches, this.branchId),
        this.fs.getDefaultBlockSize(this.outputFile));

    this.filePermission = HadoopUtils.deserializeWriterFilePermissions(properties, this.numBranches, this.branchId);

    this.dirPermission = HadoopUtils.deserializeWriterDirPermissions(properties, this.numBranches, this.branchId);

    this.group = Optional.fromNullable(properties.getProp(ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_GROUP_NAME, this.numBranches, this.branchId)));

    // Create the parent directory of the output file if it does not exist
    WriterUtils.mkdirsWithRecursivePermission(this.fs, this.outputFile.getParent(), this.dirPermission);
    this.bytesWritten = Optional.absent();
  }

  /**
   * Create the staging output file and an {@link OutputStream} to write to the file.
   *
   * @return an {@link OutputStream} to write to the staging file
   * @throws IOException if it fails to create the file and the {@link OutputStream}
   */
  protected OutputStream createStagingFileOutputStream()
      throws IOException {
    return this.closer.register(this.fs
        .create(this.stagingFile, this.filePermission, true, this.bufferSize, this.replicationFactor, this.blockSize,
            null));
  }

  /**
   * Set the group name of the staging output file.
   *
   * @throws IOException if it fails to set the group name
   */
  protected void setStagingFileGroup()
      throws IOException {
    Preconditions.checkArgument(this.fs.exists(this.stagingFile),
        String.format("Staging output file %s does not exist", this.stagingFile));
    if (this.group.isPresent()) {
      HadoopUtils.setGroup(this.fs, this.stagingFile, this.group.get());
    }
  }

  @Override
  public long bytesWritten()
      throws IOException {
    if (this.bytesWritten.isPresent()) {
      return this.bytesWritten.get().longValue();
    }
    return 0l;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This default implementation simply renames the staging file to the output file. If the output file
   *   already exists, it will delete it first before doing the renaming.
   * </p>
   *
   * @throws IOException if any file operation fails
   */
  @Override
  public void commit()
      throws IOException {
    this.closer.close();

    setStagingFileGroup();

    if (!this.fs.exists(this.stagingFile)) {
      throw new IOException(String.format("File %s does not exist", this.stagingFile));
    }

    FileStatus stagingFileStatus = this.fs.getFileStatus(this.stagingFile);

    // Double check permission of staging file
    if (!stagingFileStatus.getPermission().equals(this.filePermission)) {
      this.fs.setPermission(this.stagingFile, this.filePermission);
    }

    this.bytesWritten = Optional.of(Long.valueOf(stagingFileStatus.getLen()));

    LOG.info(String.format("Moving data from %s to %s", this.stagingFile, this.outputFile));
    // For the same reason as deleting the staging file if it already exists, deleting
    // the output file if it already exists prevents task retry from being blocked.
    if (this.fs.exists(this.outputFile)) {
      LOG.warn(String.format("Task output file %s already exists", this.outputFile));
      HadoopUtils.deletePath(this.fs, this.outputFile, false);
    }

    HadoopUtils.renamePath(this.fs, this.stagingFile, this.outputFile);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This default implementation simply deletes the staging file if it exists.
   * </p>
   *
   * @throws IOException if deletion of the staging file fails
   */
  @Override
  public void cleanup()
      throws IOException {
    // Delete the staging file
    if (this.fs.exists(this.stagingFile)) {
      HadoopUtils.deletePath(this.fs, this.stagingFile, false);
    }
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();

    if (this.shouldIncludeRecordCountInFileName) {
      String filePathWithRecordCount = addRecordCountToFileName();
      this.properties.appendToSetProp(this.allOutputFilesPropName, filePathWithRecordCount);
    } else {
      this.properties.appendToSetProp(this.allOutputFilesPropName, getOutputFilePath());
    }
  }

  private synchronized String addRecordCountToFileName()
      throws IOException {
    String filePath = getOutputFilePath();
    String filePathWithRecordCount = IngestionRecordCountProvider.constructFilePath(filePath, recordsWritten());
    LOG.info("Renaming " + filePath + " to " + filePathWithRecordCount);
    HadoopUtils.renamePath(this.fs, new Path(filePath), new Path(filePathWithRecordCount));
    this.outputFile = new Path(filePathWithRecordCount);
    return filePathWithRecordCount;
  }

  @Override
  public State getFinalState() {
    State state = new State();

    state.setProp("RecordsWritten", recordsWritten());
    try {
      state.setProp("BytesWritten", bytesWritten());
    } catch (Exception exception) {
      // If Writer fails to return bytesWritten, it might not be implemented, or implemented incorrectly.
      // Omit property instead of failing.
    }

    return state;
  }

  /**
   * Get the output file path.
   *
   * @return the output file path
   */
  public String getOutputFilePath() {
    return this.outputFile.toString();
  }

  /**
   * Get the fully-qualified output file path.
   *
   * @return the fully-qualified output file path
   */
  public String getFullyQualifiedOutputFilePath() {
    return this.fs.makeQualified(this.outputFile).toString();
  }

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.writerAttemptIdOptional.isPresent() && this.getClass() == FsDataWriter.class;
  }
}
