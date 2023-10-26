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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.typesafe.config.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemApt;
import org.apache.gobblin.temporal.ddm.work.styles.JobStateful;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.SerializationUtils;


@Slf4j
public class Help {
  public static final int MAX_DESERIALIZATION_FS_LOAD_ATTEMPTS = 5;
  public static final int LOG_CACHE_STATS_EVERY_N_ACCESSES = 1000;
  public static final String AZKABAN_FLOW_EXEC_ID_KEY = "azkaban.flow.execid";
  public static final String USER_TO_PROXY_KEY = "user.to.proxy";

  // treat `JobState` as immutable and cache, for reuse among activities executed by the same worker
  private static final transient Cache<Path, JobState> jobStateByPath = CacheBuilder.newBuilder().recordStats().build();
  private static final transient AtomicInteger jobStateAccessCount = new AtomicInteger(0);

  private Help() {}

  public static String qualifyNamePerExec(String name, FileSystemJobStateful f, Config workerConfig) {
    return name + "_" + calcPerExecQualifier(f, workerConfig);
  }

  public static String qualifyNamePerExec(String name, Config workerConfig) {
    return name + "_" + calcPerExecQualifier(workerConfig);
  }

  public static String calcPerExecQualifier(FileSystemJobStateful f, Config workerConfig) {
    Optional<String> optFlowExecId = Optional.empty();
    try {
      optFlowExecId = Optional.of(loadJobState(f).getProp(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, null));
    } catch (IOException e) {
      log.warn("unable to loadJobState", e);
    }
    return optFlowExecId.map(x -> x + "_").orElse("") + calcPerExecQualifier(workerConfig);
  }

  public static String calcPerExecQualifier(Config workerConfig) {
    String userToProxy = workerConfig.hasPath(USER_TO_PROXY_KEY)
        ? workerConfig.getString(USER_TO_PROXY_KEY) : "";
    String azFlowExecId = workerConfig.hasPath(AZKABAN_FLOW_EXEC_ID_KEY)
        ? workerConfig.getString(AZKABAN_FLOW_EXEC_ID_KEY) : UUID.randomUUID().toString();
    return userToProxy + "_" + azFlowExecId;
  }

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

  public static FileSystem loadFileSystemForce(FileSystemApt a) throws IOException {
    return loadFileSystemForUriForce(a.getFileSystemUri(), a.getFileSystemConfig());
  }

  public static FileSystem loadFileSystemForUriForce(URI fsUri, State fsConfig) throws IOException {
    // TODO - determine whether this works... unclear whether it led to "FS closed", or that had another cause...
    // return HadoopUtils.getFileSystem(fsUri, fsConfig);
    Configuration conf = HadoopUtils.getConfFromState(fsConfig);
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    return FileSystem.get(fsUri, conf);
  }

  public static JobState loadJobState(FileSystemJobStateful f) throws IOException {
    try (FileSystem fs = loadFileSystemForce(f)) {
      return loadJobState(f, fs);
    }
  }

  public static JobState loadJobState(JobStateful js, FileSystem fs) throws IOException {
    try {
      incrementJobStateAccess();
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
    try (FileSystem fs = loadFileSystemForce(f)) {
      return loadJobStateWithRetries(f, fs);
    }
  }

  public static JobState loadJobStateWithRetries(FileSystemJobStateful f, FileSystem fs) throws IOException {
    try {
      incrementJobStateAccess();
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

  // public static StateStore<TaskState> openTaskStateStore(JobStateful js, FileSystem fs) throws IOException {
  //   return JobStateUtils.openTaskStateStore(loadJobState(js, fs), fs);
  public static StateStore<TaskState> openTaskStateStore(FileSystemJobStateful js, FileSystem fs) throws IOException {
    return JobStateUtils.openTaskStateStoreUncached(loadJobState(js), fs);
  }

  private static void incrementJobStateAccess() {
    int numAccesses = jobStateAccessCount.getAndIncrement();
    if (numAccesses % LOG_CACHE_STATS_EVERY_N_ACCESSES == 0) {
      log.info("JobState(numAccesses: {}) - {}", numAccesses, jobStateByPath.stats());
    }
  }
}
