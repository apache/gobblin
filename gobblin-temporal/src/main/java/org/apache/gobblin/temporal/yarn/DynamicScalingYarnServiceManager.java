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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;
import com.google.common.util.concurrent.AbstractIdleService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.FsScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.temporal.loadgen.dynamic.DummyScalingDirectiveSource;


/**
 * This class manages the dynamic scaling of the {@link YarnService} by periodically polling for scaling directives and passing
 * the latest scaling directives to the {@link DynamicScalingYarnService} for processing.
 */
@Slf4j
public class DynamicScalingYarnServiceManager extends AbstractIdleService {

  private final String DYNAMIC_SCALING_PREFIX = GobblinTemporalConfigurationKeys.PREFIX + "dynamic.scaling.";
  private final String DYNAMIC_SCALING_DIRECTIVES_DIR = DYNAMIC_SCALING_PREFIX + "directives.dir";
  private final String DYNAMIC_SCALING_ERRORS_DIR = DYNAMIC_SCALING_PREFIX + "errors.dir";
  private final String DYNAMIC_SCALING_POLLING_INTERVAL = DYNAMIC_SCALING_PREFIX + "polling.interval";
  private final int DEFAULT_DYNAMIC_SCALING_POLLING_INTERVAL_SECS = 60;
  private final Config config;
  private final DynamicScalingYarnService dynamicScalingYarnService;
  private final ScheduledExecutorService dynamicScalingExecutor;
  private final FileSystem fs;

  public DynamicScalingYarnServiceManager(GobblinTemporalApplicationMaster appMaster) {
    this.config = appMaster.getConfig();
    if (appMaster.get_yarnService() instanceof DynamicScalingYarnService) {
      this.dynamicScalingYarnService = (DynamicScalingYarnService) appMaster.get_yarnService();
    } else {
      String errorMsg = "Failure while getting YarnService Instance from GobblinTemporalApplicationMaster::get_yarnService()"
          + " YarnService is not an instance of DynamicScalingYarnService";
      log.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    this.dynamicScalingExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(com.google.common.base.Optional.of(log),
            com.google.common.base.Optional.of("DynamicScalingExecutor")));
    this.fs = appMaster.getFs();
  }

  @Override
  protected void startUp() {
    int scheduleInterval = ConfigUtils.getInt(this.config, DYNAMIC_SCALING_POLLING_INTERVAL,
        DEFAULT_DYNAMIC_SCALING_POLLING_INTERVAL_SECS);

//    ScalingDirectiveSource fsScalingDirectiveSource = new FsScalingDirectiveSource(
//        this.fs,
//        this.config.getString(DYNAMIC_SCALING_DIRECTIVES_DIR),
//        Optional.ofNullable(this.config.getString(DYNAMIC_SCALING_ERRORS_DIR))
//    );

    // TODO: remove this line later
    //  Using for testing purposes only
    ScalingDirectiveSource scalingDirectiveSource = new DummyScalingDirectiveSource();

    log.info("Starting the " + this.getClass().getSimpleName());
    log.info("Scheduling the dynamic scaling task with an interval of {} seconds", scheduleInterval);

    this.dynamicScalingExecutor.scheduleAtFixedRate(
        new GetScalingDirectivesRunnable(this.dynamicScalingYarnService, scalingDirectiveSource),
        scheduleInterval, scheduleInterval, TimeUnit.SECONDS
    );
  }

  @Override
  protected void shutDown() {
    log.info("Stopping the " + this.getClass().getSimpleName());
    ExecutorsUtils.shutdownExecutorService(this.dynamicScalingExecutor, com.google.common.base.Optional.of(log));
  }

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
        if (!scalingDirectives.isEmpty()) {
          dynamicScalingYarnService.reviseWorkforcePlanAndRequestNewContainers(scalingDirectives);
        }
      } catch (IOException e) {
        log.error("Failed to get scaling directives", e);
      } catch (Throwable t) {
        log.error("Suppressing error from GetScalingDirectivesRunnable.run()", t);
      }
    }
  }
}
