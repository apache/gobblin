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

import static org.apache.gobblin.util.HadoopUtils.FS_SCHEMES_NON_ATOMIC;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.gobblin.util.hadoop.GobblinSequenceFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.WritableShimSerialization;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.base.Predicate;



/**
 * An implementation of {@link StateStore} backed by a {@link FileSystem}.
 *
 * <p>
 *     This implementation uses Hadoop {@link org.apache.hadoop.io.SequenceFile}
 *     to store {@link State}s. Each store maps to one directory, and each
 *     table maps to one file under the store directory. Keys are state IDs
 *     (see {@link State#getId()}), and values are objects of {@link State} or
 *     any of its extensions. Keys will be empty strings if state IDs are not set
 *     (i.e., {@link State#getId()} returns <em>null</em>). In this case, the
 *     {@link FsStateStore#get(String, String, String)} method may not work.
 * </p>
 *
 * @param <T> state object type
 *
 * @author Yinan Li
 */
public class FsStateStore<T extends State> implements StateStore<T> {

  public static final String TMP_FILE_PREFIX = "_tmp_";

  protected final Configuration conf;
  protected final FileSystem fs;
  protected boolean useTmpFileForPut;

  // Root directory for the task state store
  protected final String storeRootDir;

  // Class of the state objects to be put into the store
  private final Class<T> stateClass;

  public FsStateStore(String fsUri, String storeRootDir, Class<T> stateClass) throws IOException {
    this.conf = getConf(null);
    this.fs = FileSystem.get(URI.create(fsUri), this.conf);
    this.useTmpFileForPut = !FS_SCHEMES_NON_ATOMIC.contains(this.fs.getUri().getScheme());
    this.storeRootDir = storeRootDir;
    this.stateClass = stateClass;
  }

  /**
   * Get a Hadoop configuration that understands how to (de)serialize WritableShim objects.
   */
  private Configuration getConf(Configuration otherConf) {
    Configuration conf;
    if (otherConf == null) {
      conf = new Configuration();
    } else {
      conf = new Configuration(otherConf);
    }

    WritableShimSerialization.addToHadoopConfiguration(conf);

    return conf;
  }

  public FsStateStore(FileSystem fs, String storeRootDir, Class<T> stateClass) {
    this.fs = fs;
    this.useTmpFileForPut = !FS_SCHEMES_NON_ATOMIC.contains(this.fs.getUri().getScheme());
    this.conf = getConf(this.fs.getConf());
    this.storeRootDir = storeRootDir;
    this.stateClass = stateClass;
  }

  public FsStateStore(String storeUrl, Class<T> stateClass) throws IOException {
    this.conf = getConf(null);
    Path storePath = new Path(storeUrl);
    this.fs = storePath.getFileSystem(this.conf);
    this.useTmpFileForPut = !FS_SCHEMES_NON_ATOMIC.contains(this.fs.getUri().getScheme());
    this.storeRootDir = storePath.toUri().getPath();
    this.stateClass = stateClass;
  }

  @Override
  public boolean create(String storeName) throws IOException {
    Path storePath = new Path(this.storeRootDir, storeName);
    return this.fs.exists(storePath) || this.fs.mkdirs(storePath, new FsPermission((short) 0755));
  }

  @Override
  public boolean create(String storeName, String tableName) throws IOException {
    Path storePath = new Path(this.storeRootDir, storeName);
    if (!this.fs.exists(storePath) && !create(storeName)) {
      return false;
    }

    Path tablePath = new Path(storePath, tableName);
    if (this.fs.exists(tablePath)) {
      throw new IOException(String.format("State file %s already exists for table %s", tablePath, tableName));
    }

    return this.fs.createNewFile(tablePath);
  }

  @Override
  public boolean exists(String storeName, String tableName) throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    return this.fs.exists(tablePath);
  }

  /**
   * See {@link StateStore#put(String, String, T)}.
   *
   * <p>
   *   This implementation does not support putting the state object into an existing store as
   *   append is to be supported by the Hadoop SequenceFile (HADOOP-7139).
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
      HadoopUtils.renamePath(this.fs, tmpTablePath, tablePath);
    }
  }

  /**
   * See {@link StateStore#putAll(String, String, Collection)}.
   *
   * <p>
   *   This implementation does not support putting the state objects into an existing store as
   *   append is to be supported by the Hadoop SequenceFile (HADOOP-7139).
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
      HadoopUtils.renamePath(this.fs, tmpTablePath, tablePath);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get(String storeName, String tableName, String stateId) throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath)) {
      return null;
    }

    Closer closer = Closer.create();
    try {
      @SuppressWarnings("deprecation")
      GobblinSequenceFileReader reader = closer.register(new GobblinSequenceFileReader(this.fs, tablePath, this.conf));
      try {
        Text key = new Text();
        T state = this.stateClass.newInstance();
        while (reader.next(key)) {
          state = (T)reader.getCurrentValue(state);
          if (key.toString().equals(stateId)) {
            return state;
          }
        }
      } catch (Exception e) {
        throw new IOException("failure retrieving state from storeName " + storeName + " tableName " + tableName, e);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<T> getAll(String storeName, String tableName) throws IOException {
    List<T> states = Lists.newArrayList();

    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath)) {
      return states;
    }

    Closer closer = Closer.create();
    try {
      @SuppressWarnings("deprecation")
      GobblinSequenceFileReader reader = closer.register(new GobblinSequenceFileReader(this.fs, tablePath, this.conf));
      try {
        Text key = new Text();
        T state = this.stateClass.newInstance();
        while (reader.next(key)) {
          state = (T)reader.getCurrentValue(state);
          states.add(state);
          // We need a new object for each read state
          state = this.stateClass.newInstance();
        }
      } catch (Exception e) {
        throw new IOException("failure retrieving state from storeName " + storeName + " tableName " + tableName, e);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    return states;
  }

  @Override
  public List<T> getAll(String storeName) throws IOException {
    List<T> states = Lists.newArrayList();

    Path storePath = new Path(this.storeRootDir, storeName);
    if (!this.fs.exists(storePath)) {
      return states;
    }

    for (FileStatus status : this.fs.listStatus(storePath)) {
      states.addAll(getAll(storeName, status.getPath().getName()));
    }

    return states;
  }

  @Override
  public List<String> getTableNames(String storeName, Predicate<String> predicate) throws IOException {
    List<String> names = Lists.newArrayList();

    Path storePath = new Path(this.storeRootDir, storeName);
    if (!this.fs.exists(storePath)) {
      return names;
    }

    for (FileStatus status : this.fs.listStatus(storePath)) {
      if (predicate.apply(status.getPath().getName())) {
        names.add(status.getPath().getName());
      }
    }

    return names;
  }

  @Override
  public void createAlias(String storeName, String original, String alias) throws IOException {
    Path originalTablePath = new Path(new Path(this.storeRootDir, storeName), original);
    if (!this.fs.exists(originalTablePath)) {
      throw new IOException(String.format("State file %s does not exist for table %s", originalTablePath, original));
    }

    Path aliasTablePath = new Path(new Path(this.storeRootDir, storeName), alias);
    Path tmpAliasTablePath = new Path(aliasTablePath.getParent(), new Path(TMP_FILE_PREFIX, aliasTablePath.getName()));
    // Make a copy of the original table as a work-around because
    // Hadoop version 1.2.1 has no support for symlink yet.
    HadoopUtils.copyFile(this.fs, originalTablePath, this.fs, aliasTablePath, tmpAliasTablePath, true, this.conf);
  }

  @Override
  public void delete(String storeName, String tableName) throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (this.fs.exists(tablePath)) {
      this.fs.delete(tablePath, false);
    }
  }

  @Override
  public void delete(String storeName) throws IOException {
    Path storePath = new Path(this.storeRootDir, storeName);
    if (this.fs.exists(storePath)) {
      this.fs.delete(storePath, true);
    }
  }
}
