package org.apache.gobblin.temporal.yarn;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.FsScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.StaffingDeltas;
import org.apache.gobblin.temporal.dynamic.WorkforcePlan;
import org.apache.gobblin.temporal.dynamic.WorkforceStaffing;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;


@Slf4j
public class YarnServiceDynamicScalingManager extends AbstractIdleService {

  private final String DYNAMIC_SCALING_PREFIX = GobblinTemporalConfigurationKeys.PREFIX + "dynamic.scaling.";
  private final String DYNAMIC_SCALING_DIRECTIVES_DIR = DYNAMIC_SCALING_PREFIX + "directives.dir";
  private final String DYNAMIC_SCALING_ERRORS_DIR = DYNAMIC_SCALING_PREFIX + "errors.dir";
  private final String DYNAMIC_SCALING_INITIAL_DELAY = DYNAMIC_SCALING_PREFIX + "initial.delay";
  private final int DEFAULT_DYNAMIC_SCALING_INITIAL_DELAY_SECS = 60;
  private final String DYNAMIC_SCALING_POLLING_INTERVAL = DYNAMIC_SCALING_PREFIX + "polling.interval";
  private final int DEFAULT_DYNAMIC_SCALING_POLLING_INTERVAL_SECS = 60;
  private final Config config;
  private final YarnService yarnService;
  private final ScheduledExecutorService dynamicScalingExecutor;
  private final FileSystem fs;

  public YarnServiceDynamicScalingManager(GobblinTemporalApplicationMaster appMaster) {
    this.config = appMaster.getConfig();
    this.yarnService = appMaster.get_yarnService();
    this.dynamicScalingExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("DynamicScalingExecutor")));
    this.fs = appMaster.getFs();
  }

  @Override
  protected void startUp() {
    int scheduleInterval = ConfigUtils.getInt(this.config, DYNAMIC_SCALING_POLLING_INTERVAL,
        DEFAULT_DYNAMIC_SCALING_POLLING_INTERVAL_SECS);
    int initialDelay = ConfigUtils.getInt(this.config, DYNAMIC_SCALING_INITIAL_DELAY,
        DEFAULT_DYNAMIC_SCALING_INITIAL_DELAY_SECS);

    Config baselineConfig = ConfigFactory.empty();
    // TODO : Add required configs like initial containers, memory, cores, etc..
    WorkforcePlan workforcePlan = new WorkforcePlan(baselineConfig, 0);
    ScalingDirectiveSource scalingDirectiveSource = new FsScalingDirectiveSource(
        this.fs,
        this.config.getString(DYNAMIC_SCALING_DIRECTIVES_DIR),
        java.util.Optional.of(this.config.getString(DYNAMIC_SCALING_ERRORS_DIR))
    );

    log.info("Starting the " + YarnServiceDynamicScalingManager.class.getSimpleName());
    log.info("Scheduling the dynamic scaling task with an interval of {} seconds", scheduleInterval);

    this.dynamicScalingExecutor.scheduleAtFixedRate(
        new YarnDynamicScalingRunnable(this.yarnService, workforcePlan, scalingDirectiveSource),
        initialDelay, scheduleInterval, TimeUnit.SECONDS
    );
  }

  @Override
  protected void shutDown() {
    log.info("Stopping the " + YarnServiceDynamicScalingManager.class.getSimpleName());
    ExecutorsUtils.shutdownExecutorService(this.dynamicScalingExecutor, Optional.of(log));
  }

  @AllArgsConstructor
  static class YarnDynamicScalingRunnable implements Runnable {
    private final YarnService yarnService;
    private final WorkforcePlan workforcePlan;
    private final ScalingDirectiveSource scalingDirectiveSource;

    @Override
    public void run() {
      WorkforceStaffing workforceStaffing = workforcePlan.getStaffing();
      try {
        List<ScalingDirective> scalingDirectives = scalingDirectiveSource.getScalingDirectives();
        workforcePlan.reviseWhenNewer(scalingDirectives);
        StaffingDeltas deltas = workforcePlan.calcStaffingDeltas(workforceStaffing);
        this.yarnService.requestNewContainers(deltas);
      } catch (IOException e) {
        log.error("Failed to get scaling directives", e);
      }
    }
  }
}
