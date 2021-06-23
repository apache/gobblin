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

package org.apache.gobblin.metastore;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.HadoopUtils;


/**
 * An implementation of {@link StateStore} backed by a {@link FileSystem}.
 *
 * <p>
 *     This implementation extends {@link FsStateStore} to use
 *     {@link org.apache.hadoop.fs.FileContext} APIs to persist state to the state store.
 *     The advantage of using {@link org.apache.hadoop.fs.FileContext} is that it provides an
 *     atomic rename-with-overwrite option which allows atomic update to a previously written
 *     state file.
 * </p>
 *
 * @param <T> state object type
 *
 * @author Sudarshan Vasudevan
 */

public class FileContextBasedFsStateStore<T extends State> extends FsStateStore<T> {
  private FileContext fc;

  public FileContextBasedFsStateStore(String fsUri, String storeRootDir, Class stateClass)
      throws IOException {
    super(fsUri, storeRootDir, stateClass);
    this.fc = FileContext.getFileContext(URI.create(fsUri));
  }

  public FileContextBasedFsStateStore(FileSystem fs, String storeRootDir, Class<T> stateClass)
      throws UnsupportedFileSystemException {
    super(fs, storeRootDir, stateClass);
    this.fc = FileContext.getFileContext(this.fs.getUri());
  }

  public FileContextBasedFsStateStore(String storeUrl, Class<T> stateClass)
      throws IOException {
    super(storeUrl, stateClass);
    this.fc = FileContext.getFileContext(this.fs.getUri());
  }

  /**
   * See {@link StateStore#put(String, String, T)}.
   *
   * <p>
   *   This implementation uses {@link FileContext#rename(Path, Path, org.apache.hadoop.fs.Options.Rename...)}, with
   *   {@link org.apache.hadoop.fs.Options.Rename#OVERWRITE} set to true, to write the
   *   state to the underlying state store.
   * </p>
   */
  @Override
  protected void renamePath(Path tmpTablePath, Path tablePath) throws IOException {
    HadoopUtils.renamePath(this.fc, tmpTablePath, tablePath, true);
  }
}