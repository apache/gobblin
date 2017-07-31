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

package org.apache.gobblin.commit;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.HadoopUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link CommitStep} for renaming files within a {@link FileSystem} or between {@link FileSystem}s.
 *
 * @author Ziyang Liu
 */
@Alpha
@Slf4j
public class FsRenameCommitStep extends CommitStepBase {

  private final Path srcPath;
  private final Path dstPath;
  private final String srcFsUri;
  private final String dstFsUri;
  private final boolean overwrite;
  private transient FileSystem srcFs;
  private transient FileSystem dstFs;

  private FsRenameCommitStep(Builder<? extends Builder<?>> builder) throws IOException {
    super(builder);

    this.srcPath = builder.srcPath;
    this.dstPath = builder.dstPath;
    this.srcFs = builder.srcFs != null ? builder.srcFs
        : getFileSystem(this.props.getProp(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.srcFsUri = this.srcFs.getUri().toString();
    this.dstFs = builder.dstFs != null ? builder.dstFs
        : getFileSystem(this.props.getProp(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.dstFsUri = this.dstFs.getUri().toString();
    this.overwrite = builder.overwrite;
  }

  public static class Builder<T extends Builder<?>> extends CommitStepBase.Builder<T> {

    public Builder() {
      super();
    }

    public Builder(CommitSequence.Builder commitSequenceBuilder) {
      super(commitSequenceBuilder);
    }

    private Path srcPath;
    private Path dstPath;
    private FileSystem srcFs;
    private FileSystem dstFs;
    private boolean overwrite;

    @Override
    public T withProps(State props) {
      return super.withProps(props);
    }

    @SuppressWarnings("unchecked")
    public T from(Path srcPath) {
      this.srcPath = srcPath;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T to(Path dstPath) {
      this.dstPath = dstPath;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withSrcFs(FileSystem srcFs) {
      this.srcFs = srcFs;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withDstFs(FileSystem dstFs) {
      this.dstFs = dstFs;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T overwrite() {
      this.overwrite = true;
      return (T) this;
    }

    @Override
    public CommitStep build() throws IOException {
      Preconditions.checkNotNull(this.srcPath);
      Preconditions.checkNotNull(this.dstPath);

      return new FsRenameCommitStep(this);
    }
  }

  private FileSystem getFileSystem(String fsUri) throws IOException {
    return FileSystem.get(URI.create(fsUri), HadoopUtils.getConfFromState(this.props));
  }

  @Override
  public boolean isCompleted() throws IOException {
    if (this.dstFs == null) {
      this.dstFs = getFileSystem(this.dstFsUri);
    }
    return this.dstFs.exists(this.dstPath);
  }

  @Override
  public void execute() throws IOException {
    if (this.srcFs == null) {
      this.srcFs = getFileSystem(this.srcFsUri);
    }
    if (this.dstFs == null) {
      this.dstFs = getFileSystem(this.dstFsUri);
    }
    log.info(String.format("Moving %s to %s", this.srcPath, this.dstPath));
    HadoopUtils.movePath(this.srcFs, this.srcPath, this.dstFs, this.dstPath, this.overwrite, this.dstFs.getConf());
  }
}
