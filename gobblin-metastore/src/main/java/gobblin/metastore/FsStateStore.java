/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.State;


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
 * @author ynli
 */
public class FsStateStore implements StateStore {

  private final Configuration conf;
  private final FileSystem fs;

  // Root directory for the task state store
  private final String storeRootDir;

  // Class of the state objects to be put into the store
  private final Class<? extends State> stateClass;

  public FsStateStore(String fsUri, String storeRootDir, Class<? extends State> stateClass)
      throws IOException {
    this.conf = new Configuration();
    this.fs = FileSystem.get(URI.create(fsUri), this.conf);
    this.storeRootDir = storeRootDir;
    this.stateClass = stateClass;
  }

  public FsStateStore(FileSystem fs, String storeRootDir, Class<? extends State> stateClass)
      throws IOException {
    this.fs = fs;
    this.conf = this.fs.getConf();
    this.storeRootDir = storeRootDir;
    this.stateClass = stateClass;
  }

  public FsStateStore(String storeUrl, Class<? extends State> stateClass)
      throws IOException {
    this.conf = new Configuration();
    Path storePath = new Path(storeUrl);
    this.fs = storePath.getFileSystem(this.conf);
    this.storeRootDir = storePath.toUri().getPath();
    this.stateClass = stateClass;
  }

  @Override
  public boolean create(String storeName)
      throws IOException {
    Path storePath = new Path(this.storeRootDir, storeName);
    return this.fs.exists(storePath) || this.fs.mkdirs(storePath);
  }

  @Override
  public boolean create(String storeName, String tableName)
      throws IOException {
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
  public boolean exists(String storeName, String tableName)
      throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    return this.fs.exists(tablePath);
  }

  @Override
  public void put(String storeName, String tableName, State state)
      throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath) && !create(storeName, tableName)) {
      throw new IOException("Failed to create a state file for table " + tableName);
    }

    Closer closer = Closer.create();
    try {
      SequenceFile.Writer writer =
          closer.register(SequenceFile.createWriter(this.fs, this.conf, tablePath, Text.class, this.stateClass,
              SequenceFile.CompressionType.BLOCK, new DefaultCodec()));
      // Append will overwrite existing data, so it's not real append.
      // Real append is to be supported for SequenceFile (HADOOP-7139).
      // TODO: implement a workaround.
      writer.append(new Text(Strings.nullToEmpty(state.getId())), state);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Override
  public void putAll(String storeName, String tableName, Collection<? extends State> states)
      throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath) && !create(storeName, tableName)) {
      throw new IOException("Failed to create a state file for table " + tableName);
    }

    Closer closer = Closer.create();
    try {
      SequenceFile.Writer writer =
          closer.register(SequenceFile.createWriter(this.fs, this.conf, tablePath, Text.class, this.stateClass,
              SequenceFile.CompressionType.BLOCK, new DefaultCodec()));
      for (State state : states) {
        // Append will overwrite existing data, so it's not real append.
        // Real append is to be supported for SequenceFile (HADOOP-7139).
        // TODO: implement a workaround.
        writer.append(new Text(Strings.nullToEmpty(state.getId())), state);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  @Override
  public State get(String storeName, String tableName, String stateId)
      throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath)) {
      return null;
    }

    Closer closer = Closer.create();
    try {
      SequenceFile.Reader reader = closer.register(new SequenceFile.Reader(this.fs, tablePath, this.conf));
      try {
        Text key = new Text();
        State state = this.stateClass.newInstance();
        while (reader.next(key, state)) {
          if (key.toString().equals(stateId)) {
            return state;
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    return null;
  }

  @Override
  public List<? extends State> getAll(String storeName, String tableName)
      throws IOException {
    List<State> states = Lists.newArrayList();

    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath)) {
      return states;
    }

    Closer closer = Closer.create();
    try {
      SequenceFile.Reader reader = closer.register(new SequenceFile.Reader(this.fs, tablePath, this.conf));
      try {
        Text key = new Text();
        State state = this.stateClass.newInstance();
        while (reader.next(key, state)) {
          states.add(state);
          // We need a new object for each read state
          state = this.stateClass.newInstance();
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    return states;
  }

  @Override
  public List<? extends State> getAll(String storeName)
      throws IOException {
    List<State> states = Lists.newArrayList();

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
  public void createAlias(String storeName, String original, String alias)
      throws IOException {
    Path originalTablePath = new Path(new Path(this.storeRootDir, storeName), original);
    if (!this.fs.exists(originalTablePath)) {
      throw new IOException(String.format("State file %s does not exist for table %s", originalTablePath, original));
    }

    Path aliasTablePath = new Path(new Path(this.storeRootDir, storeName), alias);
    // Make a copy of the original table as a work-around because
    // Hadoop version 1.2.1 has no support for symlink yet.
    FileUtil.copy(this.fs, originalTablePath, this.fs, aliasTablePath, false, true, this.conf);
  }

  @Override
  public void delete(String storeName, String tableName)
      throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (this.fs.exists(tablePath)) {
      this.fs.delete(tablePath, false);
    }
  }

  @Override
  public void delete(String storeName)
      throws IOException {
    Path storePath = new Path(this.storeRootDir, storeName);
    if (this.fs.exists(storePath)) {
      this.fs.delete(storePath, true);
    }
  }
}
