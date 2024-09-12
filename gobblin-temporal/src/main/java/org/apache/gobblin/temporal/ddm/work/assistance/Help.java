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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.MDC;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemApt;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.gobblin.temporal.ddm.work.styles.JobStateful;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.SerializationUtils;


/** Various capabilities useful in implementing Distributed Data Movement (DDM) */
@Slf4j
public class Help {
  public static final int MAX_DESERIALIZATION_FS_LOAD_ATTEMPTS = 5;
  public static final int LOG_CACHE_STATS_EVERY_N_ACCESSES = 1000;
  public static final String AZKABAN_FLOW_EXEC_ID_KEY = "azkaban.flow.execid";
  public static final String USER_TO_PROXY_KEY = "user.to.proxy";
  public static final String USER_TO_PROXY_SEARCH_KEY = "userToProxy";
  public static final String GAAS_FLOW_ID_SEARCH_KEY = "gaasFlowIdSearchKey";

  // treat `JobState` as immutable and cache, for reuse among activities executed by the same worker
  private static final transient Cache<Path, JobState> jobStateByPath = CacheBuilder.newBuilder().recordStats().build();
  private static final transient AtomicInteger jobStateAccessCount = new AtomicInteger(0);

  private Help() {}

  /** @return execution-specific name, incorporating any {@link ConfigurationKeys#FLOW_EXECUTION_ID_KEY} from {@link JobState} */
  public static String qualifyNamePerExecWithFlowExecId(String name, FileSystemJobStateful f, Config workerConfig) {
    return name + "_" + calcPerExecQualifierWithOptFlowExecId(f, workerConfig);
  }

  /** @return execution-specific name, NOT incorporating {@link ConfigurationKeys#FLOW_EXECUTION_ID_KEY} */
  public static String qualifyNamePerExecWithoutFlowExecId(String name, Config workerConfig) {
    return name + "_" + calcPerExecQualifier(workerConfig);
  }

  /** @return execution-specific name, incorporating any {@link ConfigurationKeys#FLOW_EXECUTION_ID_KEY} from `config` */
  public static String qualifyNamePerExecWithFlowExecId(String name, Config config) {
    Optional<String> optFlowExecId = Optional.ofNullable(ConfigUtils.getString(config, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, null));
    return name + "_" + calcPerExecQualifierWithOptFlowExecId(optFlowExecId, config);
  }

  public static String calcPerExecQualifierWithOptFlowExecId(Optional<String> optFlowExecId, Config workerConfig) {
    return optFlowExecId.map(x -> x + "_").orElse("") + calcPerExecQualifier(workerConfig);
  }

  public static String calcPerExecQualifierWithOptFlowExecId(FileSystemJobStateful f, Config workerConfig) {
    Optional<String> optFlowExecId = Optional.empty();
    try {
      // TODO: determine whether the same could be obtained from `workerConfig` (likely much more efficiently)
      optFlowExecId = Optional.of(loadJobState(f).getProp(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, null));
    } catch (IOException e) {
      log.warn("unable to loadJobState", e);
    }
    return calcPerExecQualifierWithOptFlowExecId(optFlowExecId, workerConfig);
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
    // for reasons still not fully understood, we encountered many "FS closed" failures before disabling HDFS caching--especially as num WUs increased.
    // perhaps caching-facilitated reuse of the same FS across multiple WUs caused prior WU execs to leave the FS in a problematic state for subsequent execs
    // TODO - more investigation to sort out the true RC... and whether caching definitively is or is not possible for use here!
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

  public static StateStore<TaskState> openTaskStateStore(FileSystemJobStateful js, FileSystem fs) throws IOException {
    return JobStateUtils.openTaskStateStoreUncached(loadJobState(js), fs);
    // public static StateStore<TaskState> openTaskStateStore(JobStateful js, FileSystem fs) throws IOException {
    //   return JobStateUtils.openTaskStateStore(loadJobState(js, fs), fs);
  }

  private static void incrementJobStateAccess() {
    int numAccesses = jobStateAccessCount.getAndIncrement();
    if (numAccesses % LOG_CACHE_STATS_EVERY_N_ACCESSES == 0) {
      log.info("JobState(numAccesses: {}) - {}", numAccesses, jobStateByPath.stats());
    }
  }

  public static void propagateGaaSFlowExecutionContext(JobState jobState) {
    doGaaSFlowExecutionContextPropagation(
        jobState.getProp(ConfigurationKeys.FLOW_GROUP_KEY, "<<NOT SET>>"),
        jobState.getProp(ConfigurationKeys.FLOW_NAME_KEY, "<<NOT SET>>"),
        jobState.getProp(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "<<NOT SET>>")
    );
  }

  public static void propagateGaaSFlowExecutionContext(Properties jobProps) {
    doGaaSFlowExecutionContextPropagation(
        jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, "<<NOT SET>>"),
        jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY, "<<NOT SET>>"),
        jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "<<NOT SET>>")
    );
  }

  protected static void doGaaSFlowExecutionContextPropagation(String flowGroup, String flowName, String flowExecId) {
    // TODO: log4j2 has better syntax around conditional logging such that the key does not need to be included in the value
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, String.format("%s:%s",ConfigurationKeys.FLOW_GROUP_KEY, flowGroup));
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, String.format("%s:%s",ConfigurationKeys.FLOW_NAME_KEY, flowName));
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, String.format("%s:%s",ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecId));
  }

  /**
   * refine {@link AutomaticTroubleshooter} issues then report them to the {@link EventSubmitter} and log an issues summary via `logger`;
   * gracefully handle `null` `troubleshooter`
   */
  public static void finalizeTroubleshooting(AutomaticTroubleshooter troubleshooter, EventSubmitter eventSubmitter, Logger logger, String errCorrelator) {
    try {
      if (troubleshooter == null) {
        logger.warn("{} - No troubleshooter to report issues from automatic troubleshooter", errCorrelator);
      } else {
        Help.reportTroubleshooterIssues(troubleshooter, eventSubmitter);
      }
    } catch (TroubleshooterException e) {
      logger.error(String.format("%s - Failed to report issues from automatic troubleshooter", errCorrelator), e);
    }
  }

  /**
   * refine and report {@link AutomaticTroubleshooter} issues to the {@link EventSubmitter}; additionally {@link AutomaticTroubleshooter#logIssueSummary()}
   *
   * ATTENTION: `troubleshooter` MUST NOT be `null`
   */
  public static void reportTroubleshooterIssues(AutomaticTroubleshooter troubleshooter, EventSubmitter eventSubmitter)
      throws TroubleshooterException {
    troubleshooter.refineIssues();
    troubleshooter.logIssueSummary();
    troubleshooter.reportJobIssuesAsEvents(eventSubmitter);
  }
}
