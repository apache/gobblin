/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.aws;

import java.util.List;

import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Region;
import com.google.common.base.Optional;


/**
 * Class that handles Helix shutdown response and consequently shutdowns Amazon AutoScaling group.
 *
 * @author Abhishek Tiwari
 */
public class AWSShutdownHandler extends AsyncCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(AWSShutdownHandler.class);

  private static final boolean SHOULD_FORCE_DELETE_ASG_DEFAULT = false;

  private final AWSClusterSecurityManager awsClusterSecurityManager;
  private final Region region;
  private final Optional<List<String>> optionalLaunchConfigurationNames;
  private final Optional<List<String>> optionalAutoScalingGroupNames;

  public AWSShutdownHandler(AWSClusterSecurityManager awsClusterSecurityManager,
      Region region,
      Optional<List<String>> optionalLaunchConfigurationNames,
      Optional<List<String>> optionalAutoScalingGroupNames) {
    this.awsClusterSecurityManager = awsClusterSecurityManager;
    this.region = region;
    this.optionalLaunchConfigurationNames = optionalLaunchConfigurationNames;
    this.optionalAutoScalingGroupNames = optionalAutoScalingGroupNames;
  }

  @Override
  public void onTimeOut() {
    LOGGER.warn("Timeout while waiting for Helix controller and participants shutdown. "
        + "Moving ahead with ungraceful shutdown of Amazon AutoScaling group");

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
        AWSSdkClient.deleteLaunchConfiguration(this.awsClusterSecurityManager,
            this.region,
            launchConfigurationName);
      }
    }
    if (optionalAutoScalingGroupNames.isPresent()) {
      for (String autoScalingGroupName : optionalAutoScalingGroupNames.get()) {
        AWSSdkClient.deleteAutoScalingGroup(this.awsClusterSecurityManager,
            this.region,
            autoScalingGroupName,
            SHOULD_FORCE_DELETE_ASG_DEFAULT);
      }
    }
  }
}
