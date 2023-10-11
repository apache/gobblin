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

package org.apache.gobblin.temporal.ddm.work.assistance;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemApt;
import org.apache.gobblin.temporal.ddm.work.styles.JobStateful;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.SerializationUtils;


@Slf4j
public class Help {
  public static final int MAX_DESERIALIZATION_FS_LOAD_ATTEMPTS = 5;

  // treat `JobState` as immutable and cache, for reuse among activities executed by the same worker
  private static final transient Cache<Path, JobState> jobStateByPath = CacheBuilder.newBuilder().build();

  private Help() {}

  public static FileSystem loadFileSystem(FileSystemApt a) throws IOException {
    // NOTE: `FileSystem.get` appears to implement caching, which should facilitate sharing among activities executing on the same worker
    return loadFileSystemForUri(a.getFileSystemUri(), a.getFileSystemConfig());
  }

  public static FileSystem loadFileSystemForUri(URI fsUri, State fsConfig) throws IOException {
    // TODO - determine whether this works... unclear whether it led to "FS closed", or that had another cause...
    // return HadoopUtils.getFileSystem(fsUri, fsConfig);
    Configuration conf = HadoopUtils.getConfFromState(fsConfig);
    return FileSystem.get(fsUri, conf);
  }

  public static JobState loadJobState(FileSystemJobStateful f) throws IOException {
    try (FileSystem fs = loadFileSystem(f)) {
      return loadJobState(f, fs);
    }
  }

  public static JobState loadJobState(JobStateful js, FileSystem fs) throws IOException {
    try {
      return jobStateByPath.get(js.getJobStatePath(), () ->
          loadJobStateUncached(js, fs)
      );
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  public static JobState loadJobStateUncached(JobStateful js, FileSystem fs) throws IOException {
    JobState jobState = new JobState();
    SerializationUtils.deserializeState(fs, js.getJobStatePath(), jobState);
    log.info("loaded jobState from '{}': {}", js.getJobStatePath(), jobState.toJsonString(true));
    return jobState;
  }

  public static JobState loadJobStateWithRetries(FileSystemJobStateful f) throws IOException {
    try (FileSystem fs = loadFileSystem(f)) {
      return loadJobStateWithRetries(f, fs);
    }
  }

  public static JobState loadJobStateWithRetries(FileSystemJobStateful f, FileSystem fs) throws IOException {
    try {
      return jobStateByPath.get(f.getJobStatePath(), () ->
          loadJobStateUncachedWithRetries(f, fs, f)
      );
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  public static JobState loadJobStateUncachedWithRetries(JobStateful js, FileSystem fs, FileSystemApt fsApt) throws IOException {
    JobState jobState = new JobState();
    deserializeStateWithRetries(fs, js.getJobStatePath(), jobState, fsApt, MAX_DESERIALIZATION_FS_LOAD_ATTEMPTS);
    log.info("loaded jobState from '{}': {}", js.getJobStatePath(), jobState.toJsonString(true));
    return jobState;
  }

  public static <T extends State> void deserializeStateWithRetries(FileSystem fs, Path path, T state, FileSystemApt fsApt)
      throws IOException {
    deserializeStateWithRetries(fs, path, state, fsApt, MAX_DESERIALIZATION_FS_LOAD_ATTEMPTS);
  }

  // TODO: decide whether actually necessary...  it was added in a fit of debugging "FS closed" errors
  public static <T extends State> void deserializeStateWithRetries(FileSystem fs, Path path, T state, FileSystemApt fsApt, int maxAttempts)
      throws IOException {
    for (int i = 0; i < maxAttempts; ++i) {
      if (i > 0) {
        log.info("reopening FS '{}' to retry ({}) deserialization (attempt {})", fsApt.getFileSystemUri(),
            state.getClass().getSimpleName(), i);
        fs = Help.loadFileSystem(fsApt);
      }
      try {
        SerializationUtils.deserializeState(fs, path, state);
        return;
      } catch (IOException ioe) {
        if (ioe.getMessage().equals("Filesystem closed") && i < maxAttempts - 1) {
          continue;
        } else {
          throw ioe;
        }
      }
    }
  }

  public static StateStore<TaskState> openTaskStateStore(FileSystemJobStateful f) throws IOException {
    try (FileSystem fs = Help.loadFileSystem(f)) {
      return JobStateUtils.openTaskStateStore(Help.loadJobState(f, fs), fs);
    }
  }

  public static StateStore<TaskState> openTaskStateStore(JobStateful js, FileSystem fs) throws IOException {
    return JobStateUtils.openTaskStateStore(loadJobState(js, fs), fs);
  }
}
