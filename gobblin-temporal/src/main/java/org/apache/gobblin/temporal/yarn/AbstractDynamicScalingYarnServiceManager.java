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

package org.apache.gobblin.temporal.yarn;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.collections.CollectionUtils;

import com.typesafe.config.Config;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;

/**
 * This class manages the dynamic scaling of the {@link YarnService} by periodically polling for scaling directives and passing
 * the latest scaling directives to the {@link DynamicScalingYarnService} for processing.
 *
 * This is an abstract class that provides the basic functionality for managing dynamic scaling. Subclasses should implement
 * {@link #createScalingDirectiveSource()} to provide a {@link ScalingDirectiveSource} that will be used to get scaling directives.
 *
 * The actual implemented class needs to be passed as value of config {@link org.apache.gobblin.yarn.GobblinYarnConfigurationKeys#APP_MASTER_SERVICE_CLASSES}
 */
@Slf4j
public abstract class AbstractDynamicScalingYarnServiceManager extends AbstractIdleService {

  protected final Config config;
  protected final String applicationId;
  private final DynamicScalingYarnService dynamicScalingYarnService;
  private final ScheduledExecutorService dynamicScalingExecutor;

  public AbstractDynamicScalingYarnServiceManager(GobblinTemporalApplicationMaster appMaster) {
    this.config = appMaster.getConfig();
    this.applicationId = appMaster.getApplicationId();
    if (appMaster.get_yarnService() instanceof DynamicScalingYarnService) {
      this.dynamicScalingYarnService = (DynamicScalingYarnService) appMaster.get_yarnService();
    } else {
      String errorMsg = "Failure while getting YarnService Instance from GobblinTemporalApplicationMaster::get_yarnService()"
          + " YarnService {" + appMaster.get_yarnService().getClass().getName() + "} is not an instance of DynamicScalingYarnService";
      log.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    this.dynamicScalingExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log),
            Optional.of("DynamicScalingExecutor")));
  }

  @Override
  protected void startUp() throws IOException {
    int scheduleInterval = ConfigUtils.getInt(this.config, GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_POLLING_INTERVAL_SECS,
        GobblinTemporalConfigurationKeys.DEFAULT_DYNAMIC_SCALING_POLLING_INTERVAL_SECS);
    log.info("Starting the {} with re-scaling interval of {} seconds", this.getClass().getSimpleName(), scheduleInterval);

    this.dynamicScalingExecutor.scheduleAtFixedRate(
        new GetScalingDirectivesRunnable(this.dynamicScalingYarnService, createScalingDirectiveSource()),
        scheduleInterval, scheduleInterval, TimeUnit.SECONDS
    );
  }

  @Override
  protected void shutDown() throws IOException {
    log.info("Stopping the " + this.getClass().getSimpleName());
    ExecutorsUtils.shutdownExecutorService(this.dynamicScalingExecutor, Optional.of(log));
  }

  /**
   * Create a {@link ScalingDirectiveSource} to use for getting scaling directives.
   */
  protected abstract ScalingDirectiveSource createScalingDirectiveSource() throws IOException;

  /**
   * A {@link Runnable} that gets the scaling directives from the {@link ScalingDirectiveSource} and passes them to the
   * {@link DynamicScalingYarnService} for processing.
   */
  @AllArgsConstructor
  static class GetScalingDirectivesRunnable implements Runnable {
    private final DynamicScalingYarnService dynamicScalingYarnService;
    private final ScalingDirectiveSource scalingDirectiveSource;

    @Override
    public void run() {
      try {
        List<ScalingDirective> scalingDirectives = scalingDirectiveSource.getScalingDirectives();
        if (CollectionUtils.isNotEmpty(scalingDirectives)) {
          dynamicScalingYarnService.reviseWorkforcePlanAndRequestNewContainers(scalingDirectives);
        }
      } catch (FileNotFoundException fnfe) {
        log.warn("Failed to get scaling directives - " + fnfe.getMessage()); // important message, but no need for a stack trace
      } catch (IOException e) {
        log.error("Failed to get scaling directives", e);
      } catch (Throwable t) {
        log.error("Unexpected error with dynamic scaling via directives", t);
      }
    }
  }
}
