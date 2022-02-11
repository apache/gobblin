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
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.SpeculativeAttemptAwareConstruct;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.EncryptionFactory;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableDatasetMetadata;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.data.management.copy.OwnerAndPermission;
import org.apache.gobblin.data.management.copy.recovery.RecoveryHelper;
import org.apache.gobblin.data.management.copy.splitter.DistcpFileSplitter;
import org.apache.gobblin.instrumented.writer.InstrumentedDataWriter;
import org.apache.gobblin.state.ConstructState;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.FinalState;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.util.io.StreamCopier;
import org.apache.gobblin.util.io.StreamThrottler;
import org.apache.gobblin.util.io.ThrottledInputStream;
import org.apache.gobblin.writer.DataWriter;


/**
 * A {@link DataWriter} to write {@link FileAwareInputStream}
 */
@Slf4j
public class FileAwareInputStreamDataWriter extends InstrumentedDataWriter<FileAwareInputStream> implements FinalState, SpeculativeAttemptAwareConstruct {

  public static final String GOBBLIN_COPY_BYTES_COPIED_METER = "gobblin.copy.bytesCopiedMeter";
  public static final String GOBBLIN_COPY_CHECK_FILESIZE = "gobblin.copy.checkFileSize";
  // setting GOBBLIN_COPY_CHECK_FILESIZE to true may result in failures because the calculation of
  // expected bytes to be copied and actual bytes copied may have bugs
  public static final boolean DEFAULT_GOBBLIN_COPY_CHECK_FILESIZE = false;
  public static final String GOBBLIN_COPY_TASK_OVERWRITE_ON_COMMIT = "gobblin.copy.task.overwrite.on.commit";
  public static final boolean DEFAULT_GOBBLIN_COPY_TASK_OVERWRITE_ON_COMMIT = false;

  protected final AtomicLong bytesWritten = new AtomicLong();
  protected final AtomicLong filesWritten = new AtomicLong();
  protected final WorkUnitState state;
  protected final FileSystem fs;
  protected final Path stagingDir;
  protected final Path outputDir;
  private final Map<String, Object> encryptionConfig;
  protected CopyableDatasetMetadata copyableDatasetMetadata;
  protected final RecoveryHelper recoveryHelper;
  protected final SharedResourcesBroker<GobblinScopeTypes> taskBroker;
  protected final int bufferSize;
  private final boolean checkFileSize;
  private final Options.Rename renameOptions;
  private final URI uri;
  private final Configuration conf;

  protected final Meter copySpeedMeter;

  protected final Optional<String> writerAttemptIdOptional;
  /**
   * The copyable file in the WorkUnit might be modified by converters (e.g. output extensions added / removed).
   * This field is set when {@link #write} is called, and points to the actual, possibly modified {@link org.apache.gobblin.data.management.copy.CopyEntity}
   * that was written by this writer.
   */
  protected Optional<CopyableFile> actualProcessedCopyableFile;

  public FileAwareInputStreamDataWriter(State state, int numBranches, int branchId, String writerAttemptId)
      throws IOException {
    this(state, null, numBranches, branchId, writerAttemptId);
  }

  public FileAwareInputStreamDataWriter(State state, FileSystem fileSystem, int numBranches, int branchId, String writerAttemptId)
      throws IOException {
    super(state);

    if (numBranches > 1) {
      throw new IOException("Distcp can only operate with one branch.");
    }

    if (!(state instanceof WorkUnitState)) {
      throw new RuntimeException(String.format("Distcp requires a %s on construction.", WorkUnitState.class.getSimpleName()));
    }
    this.state = (WorkUnitState) state;

    this.taskBroker = this.state.getTaskBroker();

    this.writerAttemptIdOptional = Optional.fromNullable(writerAttemptId);

    String uriStr = this.state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
        ConfigurationKeys.LOCAL_FS_URI);

    this.conf = WriterUtils.getFsConfiguration(state);
    this.uri = URI.create(uriStr);
    if (fileSystem != null) {
      this.fs = fileSystem;
    } else {
      this.fs = FileSystem.get(uri, conf);
    }
    if (state.getPropAsBoolean(ConfigurationKeys.USER_DEFINED_STAGING_DIR_FLAG,false)) {
      this.stagingDir = new Path(state.getProp(ConfigurationKeys.USER_DEFINED_STATIC_STAGING_DIR));
    } else {
      this.stagingDir = this.writerAttemptIdOptional.isPresent() ? WriterUtils.getWriterStagingDir(state, numBranches, branchId, this.writerAttemptIdOptional.get())
          : WriterUtils.getWriterStagingDir(state, numBranches, branchId);
    }

    this.copyableDatasetMetadata =
        CopyableDatasetMetadata.deserialize(state.getProp(CopySource.SERIALIZED_COPYABLE_DATASET));
    this.outputDir = getOutputDir(state);
    this.recoveryHelper = new RecoveryHelper(this.fs, state);
    this.actualProcessedCopyableFile = Optional.absent();

    // remove the old metric which counts how many bytes are copied, because in case of retries, this can give incorrect value
    if (getMetricContext().getMetrics().containsKey(GOBBLIN_COPY_BYTES_COPIED_METER)) {
      getMetricContext().remove(GOBBLIN_COPY_BYTES_COPIED_METER);
    }

    this.copySpeedMeter = getMetricContext().meter(GOBBLIN_COPY_BYTES_COPIED_METER);

    this.bufferSize = state.getPropAsInt(CopyConfiguration.BUFFER_SIZE, StreamCopier.DEFAULT_BUFFER_SIZE);
    this.encryptionConfig = EncryptionConfigParser
        .getConfigForBranch(EncryptionConfigParser.EntityType.WRITER, this.state, numBranches, branchId);

    this.checkFileSize = state.getPropAsBoolean(GOBBLIN_COPY_CHECK_FILESIZE, DEFAULT_GOBBLIN_COPY_CHECK_FILESIZE);
    boolean taskOverwriteOnCommit = state.getPropAsBoolean(GOBBLIN_COPY_TASK_OVERWRITE_ON_COMMIT, DEFAULT_GOBBLIN_COPY_TASK_OVERWRITE_ON_COMMIT);
    if (taskOverwriteOnCommit) {
      this.renameOptions = Options.Rename.OVERWRITE;
    } else {
      this.renameOptions = Options.Rename.NONE;
    }
  }

  public FileAwareInputStreamDataWriter(State state, int numBranches, int branchId)
      throws IOException {
    this(state, numBranches, branchId, null);
  }

  @Override
  public final void writeImpl(FileAwareInputStream fileAwareInputStream)
      throws IOException {
    CopyableFile copyableFile = fileAwareInputStream.getFile();
    if (encryptionConfig != null) {
      copyableFile.setDestination(PathUtils.addExtension(copyableFile.getDestination(),
          "." + EncryptionConfigParser.getEncryptionType(encryptionConfig)));
    }
    Path stagingFile = getStagingFilePath(copyableFile);
    if (this.actualProcessedCopyableFile.isPresent()) {
      throw new IOException(this.getClass().getCanonicalName() + " can only process one file and cannot be reused.");
    }

    this.fs.mkdirs(stagingFile.getParent());
    writeImpl(fileAwareInputStream.getInputStream(), stagingFile, copyableFile, fileAwareInputStream);

    this.actualProcessedCopyableFile = Optional.of(copyableFile);
    this.filesWritten.incrementAndGet();
  }

  /**
   * Write the contents of input stream into staging path.
   *
   * <p>
   *   WriteAt indicates the path where the contents of the input stream should be written. When this method is called,
   *   the path writeAt.getParent() will exist already, but the path writeAt will not exist. When this method is returned,
   *   the path writeAt must exist. Any data written to any location other than writeAt or a descendant of writeAt
   *   will be ignored.
   * </p>
   *
   * @param inputStream {@link FSDataInputStream} whose contents should be written to staging path.
   * @param writeAt {@link Path} at which contents should be written.
   * @param copyableFile {@link org.apache.gobblin.data.management.copy.CopyEntity} that generated this copy operation.
   * @param record The actual {@link FileAwareInputStream} passed to the write method.
   * @throws IOException
   */
  protected void writeImpl(InputStream inputStream, Path writeAt, CopyableFile copyableFile,
      FileAwareInputStream record) throws IOException {

    final short replication = this.state.getPropAsShort(ConfigurationKeys.WRITER_FILE_REPLICATION_FACTOR,
        copyableFile.getReplication(this.fs));
    final long blockSize = copyableFile.getBlockSize(this.fs);
    final long fileSize = copyableFile.getFileStatus().getLen();

    long expectedBytes = fileSize;
    Long maxBytes = null;
    // Whether writer must write EXACTLY maxBytes.
    boolean mustMatchMaxBytes = false;

    if (record.getSplit().isPresent()) {
      maxBytes = record.getSplit().get().getHighPosition() - record.getSplit().get().getLowPosition();
      if (record.getSplit().get().isLastSplit()) {
        expectedBytes = fileSize % blockSize;
        mustMatchMaxBytes = false;
      } else {
        expectedBytes = maxBytes;
        mustMatchMaxBytes = true;
      }
    }

    Predicate<FileStatus> fileStatusAttributesFilter = new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus input) {
        return input.getReplication() == replication && input.getBlockSize() == blockSize;
      }
    };
    Optional<FileStatus> persistedFile =
        this.recoveryHelper.findPersistedFile(this.state, copyableFile, fileStatusAttributesFilter);

    if (persistedFile.isPresent()) {
      log.info(String.format("Recovering persisted file %s to %s.", persistedFile.get().getPath(), writeAt));
      this.fs.rename(persistedFile.get().getPath(), writeAt);
    } else {
      // Copy empty directories
      if (copyableFile.getFileStatus().isDirectory()) {
        this.fs.mkdirs(writeAt);
        return;
      }

      OutputStream os =
          this.fs.create(writeAt, true, this.fs.getConf().getInt("io.file.buffer.size", 4096), replication, blockSize);
      if (encryptionConfig != null) {
        os = EncryptionFactory.buildStreamCryptoProvider(encryptionConfig).encodeOutputStream(os);
      }
      try {
        FileSystem defaultFS = FileSystem.get(new Configuration());
        StreamThrottler<GobblinScopeTypes> throttler =
            this.taskBroker.getSharedResource(new StreamThrottler.Factory<GobblinScopeTypes>(), new EmptyKey());
        ThrottledInputStream throttledInputStream = throttler.throttleInputStream().inputStream(inputStream)
            .sourceURI(copyableFile.getOrigin().getPath().makeQualified(defaultFS.getUri(), defaultFS.getWorkingDirectory()).toUri())
            .targetURI(this.fs.makeQualified(writeAt).toUri()).build();
        StreamCopier copier = new StreamCopier(throttledInputStream, os, maxBytes).withBufferSize(this.bufferSize);

        log.info("File {}: Starting copy", copyableFile.getOrigin().getPath());

        if (isInstrumentationEnabled()) {
          copier.withCopySpeedMeter(this.copySpeedMeter);
        }
        long numBytes = copier.copy();
        if ((this.checkFileSize || mustMatchMaxBytes) && numBytes != expectedBytes) {
          throw new IOException(String.format("Incomplete write: expected %d, wrote %d bytes.",
              expectedBytes, numBytes));
        }
        this.bytesWritten.addAndGet(numBytes);
        if (isInstrumentationEnabled()) {
          log.info("File {}: copied {} bytes, average rate: {} B/s", copyableFile.getOrigin().getPath(),
              this.copySpeedMeter.getCount(), this.copySpeedMeter.getMeanRate());
        } else {
          log.info("File {} copied.", copyableFile.getOrigin().getPath());
        }
      } catch (NotConfiguredException nce) {
        log.warn("Broker error. Some features of stream copier may not be available.", nce);
      } finally {
        os.close();
        log.info("OutputStream for file {} is closed.", writeAt);
        inputStream.close();
      }
    }
  }

  /**
   * Sets the owner/group and permission for the file in the task staging directory
   */
  protected void setFilePermissions(CopyableFile file)
      throws IOException {
    setRecursivePermission(getStagingFilePath(file), file.getDestinationOwnerAndPermission());
  }

  protected Path getStagingFilePath(CopyableFile file) {
    if (DistcpFileSplitter.isSplitWorkUnit(this.state)) {
      return new Path(this.stagingDir, DistcpFileSplitter.getSplit(this.state).get().getPartName());
    }
    return new Path(this.stagingDir, file.getDestination().getName());
  }

  protected static Path getPartitionOutputRoot(Path outputDir, CopyEntity.DatasetAndPartition datasetAndPartition) {
    return new Path(outputDir, datasetAndPartition.identifier());
  }

  public static Path getOutputFilePath(CopyableFile file, Path outputDir,
      CopyEntity.DatasetAndPartition datasetAndPartition) {
    Path destinationWithoutSchemeAndAuthority = PathUtils.getPathWithoutSchemeAndAuthority(file.getDestination());
    return new Path(getPartitionOutputRoot(outputDir, datasetAndPartition),
        PathUtils.withoutLeadingSeparator(destinationWithoutSchemeAndAuthority));
  }

  public static Path getSplitOutputFilePath(CopyableFile file, Path outputDir,
      CopyableFile.DatasetAndPartition datasetAndPartition, State workUnit) {
    if (DistcpFileSplitter.isSplitWorkUnit(workUnit)) {
      return new Path(getOutputFilePath(file, outputDir, datasetAndPartition).getParent(),
          DistcpFileSplitter.getSplit(workUnit).get().getPartName());
    } else {
      return getOutputFilePath(file, outputDir, datasetAndPartition);
    }
  }

  public static Path getOutputDir(State state) {
    return new Path(
        state.getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, 1, 0)));
  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed. It will not throw exceptions, if operations
   * cannot be executed, will warn and continue.
   */
  private void safeSetPathPermission(Path path, OwnerAndPermission ownerAndPermission) {

    try {
      if (ownerAndPermission.getFsPermission() != null) {
        this.fs.setPermission(path, ownerAndPermission.getFsPermission());
      }
    } catch (IOException ioe) {
      log.warn("Failed to set permission for directory " + path, ioe);
    }

    String owner = Strings.isNullOrEmpty(ownerAndPermission.getOwner()) ? null : ownerAndPermission.getOwner();
    String group = Strings.isNullOrEmpty(ownerAndPermission.getGroup()) ? null : ownerAndPermission.getGroup();

    try {
      if (owner != null || group != null) {
        this.fs.setOwner(path, owner, group);
      }
    } catch (IOException ioe) {
      log.warn("Failed to set owner and/or group for path " + path + " to " + owner + ":" + group, ioe);
    }
  }

  /**
   * Sets the {@link FsPermission}, owner, group for the path passed. And recursively to all directories and files under
   * it.
   */
  private void setRecursivePermission(Path path, OwnerAndPermission ownerAndPermission)
      throws IOException {
    List<FileStatus> files = FileListUtils.listPathsRecursively(this.fs, path, FileListUtils.NO_OP_PATH_FILTER);

    // Set permissions bottom up. Permissions are set to files first and then directories
    Collections.reverse(files);

    for (FileStatus file : files) {
      safeSetPathPermission(file.getPath(), addExecutePermissionsIfRequired(file, ownerAndPermission));
    }
  }

  /**
   * The method makes sure it always grants execute permissions for an owner if the <code>file</code> passed is a
   * directory. The publisher needs it to publish it to the final directory and list files under this directory.
   */
  private static OwnerAndPermission addExecutePermissionsIfRequired(FileStatus file,
      OwnerAndPermission ownerAndPermission) {

    if (ownerAndPermission.getFsPermission() == null) {
      return ownerAndPermission;
    }

    if (!file.isDir()) {
      return ownerAndPermission;
    }

    return new OwnerAndPermission(ownerAndPermission.getOwner(), ownerAndPermission.getGroup(),
        addExecutePermissionToOwner(ownerAndPermission.getFsPermission()));
  }

  static FsPermission addExecutePermissionToOwner(FsPermission fsPermission) {
    FsAction newOwnerAction = fsPermission.getUserAction().or(FsAction.EXECUTE);
    return new FsPermission(newOwnerAction, fsPermission.getGroupAction(), fsPermission.getOtherAction());
  }

  @Override
  public long recordsWritten() {
    return this.filesWritten.get();
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return this.bytesWritten.get();
  }

  /**
   * Moves the file from task staging to task output. Each task has its own staging directory but all the tasks share
   * the same task output directory.
   *
   * {@inheritDoc}
   *
   * @see DataWriter#commit()
   */
  @Override
  public void commit()
      throws IOException {

    if (!this.actualProcessedCopyableFile.isPresent()) {
      return;
    }

    CopyableFile copyableFile = this.actualProcessedCopyableFile.get();
    Path stagingFilePath = getStagingFilePath(copyableFile);
    Path outputFilePath = getSplitOutputFilePath(copyableFile, this.outputDir,
        copyableFile.getDatasetAndPartition(this.copyableDatasetMetadata), this.state);

    log.info(String.format("Committing data from %s to %s", stagingFilePath, outputFilePath));
    try {
      setFilePermissions(copyableFile);

      Iterator<OwnerAndPermission> ancestorOwnerAndPermissionIt =
          copyableFile.getAncestorsOwnerAndPermission() == null ? Iterators.emptyIterator()
              : copyableFile.getAncestorsOwnerAndPermission().iterator();

      ensureDirectoryExists(this.fs, outputFilePath.getParent(), ancestorOwnerAndPermissionIt);

      // Do not store the FileContext after doing the rename because FileContexts are not cached and a new object
      // is created for every task's commit
      FileContext.getFileContext(this.uri, this.conf).rename(stagingFilePath, outputFilePath, renameOptions);
    } catch (IOException ioe) {
      log.error("Could not commit file {}.", outputFilePath);
      // persist file
      this.recoveryHelper.persistFile(this.state, copyableFile, stagingFilePath);
      throw ioe;
    } finally {
      try {
        this.fs.delete(this.stagingDir, true);
      } catch (IOException ioe) {
        log.warn("Failed to delete staging path at " + this.stagingDir);
      }
    }
  }

  private void ensureDirectoryExists(FileSystem fs, Path path, Iterator<OwnerAndPermission> ownerAndPermissionIterator)
      throws IOException {

    if (fs.exists(path)) {
      return;
    }

    if (ownerAndPermissionIterator.hasNext()) {
      OwnerAndPermission ownerAndPermission = ownerAndPermissionIterator.next();

      if (path.getParent() != null) {
        ensureDirectoryExists(fs, path.getParent(), ownerAndPermissionIterator);
      }

      if (!fs.mkdirs(path)) {
        // fs.mkdirs returns false if path already existed. Do not overwrite permissions
        return;
      }

      if (ownerAndPermission.getFsPermission() != null) {
        log.debug("Applying permissions {} to path {}.", ownerAndPermission.getFsPermission(), path);
        fs.setPermission(path, addExecutePermissionToOwner(ownerAndPermission.getFsPermission()));
      }

      String group = ownerAndPermission.getGroup();
      String owner = ownerAndPermission.getOwner();
      if (group != null || owner != null) {
        log.debug("Applying owner {} and group {} to path {}.", owner, group, path);
        fs.setOwner(path, owner, group);
      }
    } else {
      fs.mkdirs(path);
    }
  }

  @Override
  public void cleanup()
      throws IOException {
    // Do nothing
  }

  @Override
  public State getFinalState() {
    State state = new State();
    if (this.actualProcessedCopyableFile.isPresent()) {
      CopySource.serializeCopyEntity(state, this.actualProcessedCopyableFile.get());
    }
    ConstructState constructState = new ConstructState();
    constructState.addOverwriteProperties(state);
    return constructState;
  }

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.writerAttemptIdOptional.isPresent() && this.getClass() == FileAwareInputStreamDataWriter.class;
  }
}
