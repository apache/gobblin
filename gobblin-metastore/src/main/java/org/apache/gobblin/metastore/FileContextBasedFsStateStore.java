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
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import com.google.common.base.Strings;
import com.google.common.io.Closer;

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
    this.fc = FileContext.getFileContext(new Configuration());
  }

  public FileContextBasedFsStateStore(FileSystem fs, String storeRootDir, Class<T> stateClass)
      throws UnsupportedFileSystemException {
    super(fs, storeRootDir, stateClass);
    this.fc = FileContext.getFileContext(fs.getUri());
  }

  public FileContextBasedFsStateStore(String storeUrl, Class<T> stateClass)
      throws IOException {
    super(storeUrl, stateClass);
    this.fc = FileContext.getFileContext(new Configuration());
  }

  /**
   * See {@link StateStore#put(String, String, T)}.
   *
   * <p>
   *   This implementation uses {@link FileContext#rename(Path, Path, Options.Rename...)}, with
   *   {@link org.apache.hadoop.fs.Options.Rename#OVERWRITE} set to true, to write the
   *   state to the underlying state store.
   * </p>
   */
  @Override
  public void put(String storeName, String tableName, T state) throws IOException {
    String tmpTableName = this.useTmpFileForPut ? TMP_FILE_PREFIX + tableName : tableName;
    Path tmpTablePath = new Path(new Path(this.storeRootDir, storeName), tmpTableName);

    if (!this.fs.exists(tmpTablePath) && !create(storeName, tmpTableName)) {
      throw new IOException("Failed to create a state file for table " + tmpTableName);
    }

    Closer closer = Closer.create();
    try {
      @SuppressWarnings("deprecation")
      SequenceFile.Writer writer = closer.register(SequenceFile.createWriter(this.fs, this.conf, tmpTablePath,
          Text.class, this.stateClass, SequenceFile.CompressionType.BLOCK, new DefaultCodec()));
      writer.append(new Text(Strings.nullToEmpty(state.getId())), state);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    if (this.useTmpFileForPut) {
      Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
      HadoopUtils.renamePath(this.fc, tmpTablePath, tablePath, true);
    }
  }

  /**
   * See {@link StateStore#put(String, String, T)}.
   *
   * <p>
   *   This implementation uses {@link FileContext#rename(Path, Path, Options.Rename...)}, with
   *   {@link org.apache.hadoop.fs.Options.Rename#OVERWRITE} set to true, to write the
   *   state to the underlying state store.
   * </p>
   */
  @Override
  public void putAll(String storeName, String tableName, Collection<T> states) throws IOException {
    String tmpTableName = this.useTmpFileForPut ? TMP_FILE_PREFIX + tableName : tableName;
    Path tmpTablePath = new Path(new Path(this.storeRootDir, storeName), tmpTableName);

    if (!this.fs.exists(tmpTablePath) && !create(storeName, tmpTableName)) {
      throw new IOException("Failed to create a state file for table " + tmpTableName);
    }

    Closer closer = Closer.create();
    try {
      @SuppressWarnings("deprecation")
      SequenceFile.Writer writer = closer.register(SequenceFile.createWriter(this.fs, this.conf, tmpTablePath,
          Text.class, this.stateClass, SequenceFile.CompressionType.BLOCK, new DefaultCodec()));
      for (T state : states) {
        writer.append(new Text(Strings.nullToEmpty(state.getId())), state);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    if (this.useTmpFileForPut) {
      Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
      HadoopUtils.renamePath(this.fc, tmpTablePath, tablePath, true);
    }
  }
}