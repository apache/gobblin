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
package org.apache.gobblin.cluster;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskDriver;

import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.commit.CommitStepException;


/**
 * A {@link CommitStep} that checks with Helix if a particular Helix instance is still the assigned participant for a given
 * Helix Partition. This {@link CommitStep} implementation is a safety check against Helix and is intended to be used
 * before data is published and state is committed. The primiary motivation for this {@link CommitStep} is to avoid a "split-brain"
 * scenario where a runaway Helix task continues to process a partition even though Helix has assigned the same
 * partition to a different Helix task. This can happen due to inconsistency between the state of a task as maintained
 * by Helix on ZK vs the local state of the task.
 */
@Slf4j
@Alias (value = "HelixParticipantCheck")
public class HelixAssignedParticipantCheck implements CommitStep {
  @Getter
  @VisibleForTesting
  private static volatile HelixManager helixManager = null;
  private static volatile Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
      .retryIfException()
      .withStopStrategy(StopStrategies.stopAfterAttempt(3))
      .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(3000, TimeUnit.MILLISECONDS)).build();

  private final String helixInstanceName;
  private final String helixJob;
  private final int partitionNum;
  private final Config config;

  /**
   * A method that uses the Singleton pattern to instantiate a {@link HelixManager} instance.
   * @param config
   * @return
   */
  public static void initHelixManager(Config config) throws Exception {
    if (helixManager == null) {
      synchronized (HelixAssignedParticipantCheck.class) {
        if (helixManager == null) {
          String zkConnectString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
          String clusterName = config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
          helixManager = HelixManagerFactory.getZKHelixManager(clusterName, HelixAssignedParticipantCheck.class.getSimpleName(),
              InstanceType.SPECTATOR, zkConnectString);
          helixManager.connect();
        }
      }
    }
  }

  /**
   * Refresh {@link HelixManager} instance. Invoked when the underlying ZkClient is closed causing Helix
   * APIs to throw an Exception.
   * @throws Exception
   */
  private void refreshHelixManager() throws Exception {
    synchronized (HelixAssignedParticipantCheck.class) {
      //Ensure existing instance is disconnected to close any open connections.
      helixManager.disconnect();
      helixManager = null;
      initHelixManager(config);
    }
  }

  public HelixAssignedParticipantCheck(Config config) throws Exception {
    this.config = config;
    initHelixManager(config);
    this.helixInstanceName = config.getString(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY);
    this.helixJob = config.getString(GobblinClusterConfigurationKeys.HELIX_JOB_ID_KEY);
    this.partitionNum = config.getInt(GobblinClusterConfigurationKeys.HELIX_PARTITION_ID_KEY);
  }

  /**
   * Determine whether the commit step has been completed.
   */
  @Override
  public boolean isCompleted() {
    return false;
  }

  /**
   * Execute the commit step.
   */
  @Override
  public void execute() throws CommitStepException {
    log.info(String.format("HelixParticipantCheck step called for Helix Instance: %s, Helix job: %s, Helix partition: %d",
        this.helixInstanceName, this.helixJob, this.partitionNum));

    //Query Helix to get the currently assigned participant for the Helix partitionNum
    Callable callable = () -> {
      JobContext jobContext;
      try {
        TaskDriver taskDriver = new TaskDriver(helixManager);
        jobContext = taskDriver.getJobContext(helixJob);
      } catch (Exception e) {
        log.info("Encountered exception when executing " + getClass().getSimpleName(), e);
        log.info("Refreshing Helix manager..");
        refreshHelixManager();
        //Rethrow the exception to trigger a retry.
        throw e;
      }

      if (jobContext != null) {
        String participant = jobContext.getAssignedParticipant(partitionNum);
        if (participant != null) {
          boolean isAssignedParticipant = participant.equalsIgnoreCase(helixInstanceName);
          if (!isAssignedParticipant) {
            log.info("The current helix instance is not the assigned participant. helixInstanceName={}, assignedParticipant={}",
                helixInstanceName, participant);
          }

          return isAssignedParticipant;
        }
      }
      return false;
    };

    boolean isParticipant;
    try {
      isParticipant = retryer.call(callable);
    } catch (ExecutionException | RetryException e) {
      log.error("Cannot complete participant assignment check within the retry limit due to: {}", e);
      //Set isParticipant to true; since we cannot verify the status of the Helix Participant at this time.
      isParticipant = true;
    }

    if (!isParticipant) {
      throw new CommitStepException(String.format("Helix instance %s not the assigned participant for partition %d",this.helixInstanceName, this.partitionNum));
    }
  }
}
