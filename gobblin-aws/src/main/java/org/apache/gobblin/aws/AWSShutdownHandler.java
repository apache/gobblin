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
package org.apache.gobblin.aws;

import java.util.List;

import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import org.apache.gobblin.annotation.Alpha;


/**
 * Class that handles Helix shutdown response and consequently shutdowns Amazon AutoScaling group.
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class AWSShutdownHandler extends AsyncCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(AWSShutdownHandler.class);

  private final AWSSdkClient awsSdkClient;
  private final Optional<List<String>> optionalLaunchConfigurationNames;
  private final Optional<List<String>> optionalAutoScalingGroupNames;

  public AWSShutdownHandler(AWSSdkClient awsSdkClient,
      Optional<List<String>> optionalLaunchConfigurationNames,
      Optional<List<String>> optionalAutoScalingGroupNames) {
    this.awsSdkClient = awsSdkClient;
    this.optionalLaunchConfigurationNames = optionalLaunchConfigurationNames;
    this.optionalAutoScalingGroupNames = optionalAutoScalingGroupNames;
  }

  @Override
  public void onTimeOut() {
    LOGGER.warn("Timeout while waiting for Helix controller and participants shutdown. "
        + "Moving ahead with forced shutdown of Amazon AutoScaling group");

    shutdownASG();
  }

  @Override
  public void onReplyMessage(Message message) {
    LOGGER.info("Successfully shutdown Helix controller and participants shutdown. "
        + "Moving ahead with graceful shutdown of Amazon AutoScaling group");

    shutdownASG();
  }

  private void shutdownASG() {
    if (optionalLaunchConfigurationNames.isPresent()) {
      for (String launchConfigurationName : optionalLaunchConfigurationNames.get()) {
        try {
          this.awsSdkClient.deleteLaunchConfiguration(launchConfigurationName);
        } catch (Exception e) {
          // Ignore and continue, so that we clean up as many resources as possible
          LOGGER.warn("Issue in deleting launch configuration, please delete manually: " + launchConfigurationName +
              " Continuing to cleanup AutoScalingGroups", e);
        }
      }
    }
    if (optionalAutoScalingGroupNames.isPresent()) {
      for (String autoScalingGroupName : optionalAutoScalingGroupNames.get()) {
        try {
          this.awsSdkClient.deleteAutoScalingGroup(autoScalingGroupName, false);
        } catch (Exception e1) {
          LOGGER.warn("Issue in deleting auto scaling group (in graceful mode): " + autoScalingGroupName
              + " Going to try forceful cleanup.", e1);

          try {
            // Delete forcefully
            this.awsSdkClient.deleteAutoScalingGroup(autoScalingGroupName, true);
          } catch (Exception e2) {
            // Ignore and continue, so that we clean up as many resources as possible
            LOGGER.warn("Issue in deleting auto scaling group (in forced mode), please delete manually: " +
                autoScalingGroupName + " Continuing to cleanup other resources", e2);
          }
        }
      }
    }
  }
}
