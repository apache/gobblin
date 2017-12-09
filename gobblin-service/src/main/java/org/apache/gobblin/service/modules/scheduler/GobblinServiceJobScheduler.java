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

package org.apache.gobblin.service.modules.scheduler;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.scheduler.BaseGobblinJob;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An extension to {@link JobScheduler} that is also a {@link SpecCatalogListener}.
 * {@link GobblinServiceJobScheduler} listens for new / updated {@link FlowSpec} and schedules
 * and runs them via {@link Orchestrator}.
 */
@Alpha
public class GobblinServiceJobScheduler extends JobScheduler implements SpecCatalogListener {
  protected final Logger _log;

  protected final Optional<FlowCatalog> flowCatalog;
  protected final Optional<HelixManager> helixManager;
  protected final Orchestrator orchestrator;
  @Getter
  protected final Map<String, Spec> scheduledFlowSpecs;

  @Getter
  protected volatile boolean isActive;

  public GobblinServiceJobScheduler(Config config, Optional<HelixManager> helixManager, Optional<FlowCatalog> flowCatalog,
      Optional<TopologyCatalog> topologyCatalog, Orchestrator orchestrator, SchedulerService schedulerService,
      Optional<Logger> log) throws Exception {
    super(ConfigUtils.configToProperties(config), schedulerService);

    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());

    this.flowCatalog = flowCatalog;
    this.helixManager = helixManager;
    this.orchestrator = orchestrator;
    this.scheduledFlowSpecs = Maps.newHashMap();
  }

  public GobblinServiceJobScheduler(Config config, Optional<HelixManager> helixManager, Optional<FlowCatalog> flowCatalog,
      Optional<TopologyCatalog> topologyCatalog, SchedulerService schedulerService, Optional<Logger> log) throws Exception {
    this(config, helixManager, flowCatalog, topologyCatalog, new Orchestrator(config, topologyCatalog, log),
        schedulerService, log);
  }


  public synchronized void setActive(boolean isActive) {
    if (this.isActive == isActive) {
      // No-op if already in correct state
      return;
    }

    // Since we are going to change status to isActive=true, schedule all flows
    if (isActive) {
      // Need to set active first; otherwise in the STANDBY->ACTIVE transition,
      // the onAddSpec will forward specs to the leader, which is itself.
      this.isActive = isActive;
      if (this.flowCatalog.isPresent()) {
        Collection<Spec> specs = this.flowCatalog.get().getSpecsWithTimeUpdate();
        for (Spec spec : specs) {
          onAddSpec(spec);
        }
      }
    }
    // Since we are going to change status to isActive=false, unschedule all flows
    else {
      for (Spec spec : this.scheduledFlowSpecs.values()) {
        onDeleteSpec(spec.getUri(), spec.getVersion());
      }
      // Need to set active at the end; otherwise in the ACTIVE->STANDBY transition,
      // the onDeleteSpec will forward specs to the leader, which is itself.
      this.isActive = isActive;
    }
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
  }

  /**
   * Synchronize the job scheduling because the same flowSpec can be scheduled by different threads.
   */
  @Override
  public synchronized void scheduleJob(Properties jobProps, JobListener jobListener) throws JobException {
    Map<String, Object> additionalJobDataMap = Maps.newHashMap();
    additionalJobDataMap.put(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWSPEC,
        this.scheduledFlowSpecs.get(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)));

    try {
      scheduleJob(jobProps, jobListener, additionalJobDataMap, GobblinServiceJob.class);
    } catch (Exception e) {
      throw new JobException("Failed to schedule job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  @Override
  public void runJob(Properties jobProps, JobListener jobListener) throws JobException {
    try {
      Spec flowSpec = this.scheduledFlowSpecs.get(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
      this.orchestrator.orchestrate(flowSpec);
    } catch (Exception e) {
      throw new JobException("Failed to run Spec: " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onAddSpec(Spec addedSpec) {
    if (this.helixManager.isPresent() && !this.helixManager.get().isConnected()) {
      // Specs in store will be notified when Scheduler is added as listener to FlowCatalog, so ignore
      // .. Specs if in cluster mode and Helix is not yet initialized
      _log.info("System not yet initialized. Skipping Spec Addition: " + addedSpec);
      return;
    }

    _log.info("New Flow Spec detected: " + addedSpec);

    if (addedSpec instanceof FlowSpec) {
      if (!isActive && helixManager.isPresent()) {
        _log.info("Scheduler running in slave mode, forward Spec add via Helix message to master: " + addedSpec);
        HelixUtils.sendUserDefinedMessage(ServiceConfigKeys.HELIX_FLOWSPEC_ADD, addedSpec.getUri().toString(),
            UUID.randomUUID().toString(), InstanceType.CONTROLLER, helixManager.get(), _log);
        return;
      }

      try {
        Properties jobConfig = new Properties();
        Properties flowSpecProperties = ((FlowSpec) addedSpec).getConfigAsProperties();
        jobConfig.putAll(this.properties);
        jobConfig.setProperty(ConfigurationKeys.JOB_NAME_KEY, addedSpec.getUri().toString());
        jobConfig.setProperty(ConfigurationKeys.JOB_GROUP_KEY,
            ((FlowSpec) addedSpec).getConfig().getValue(ConfigurationKeys.FLOW_GROUP_KEY).toString());
        jobConfig.setProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY,
            ConfigUtils.getString(((FlowSpec) addedSpec).getConfig(), ConfigurationKeys.FLOW_RUN_IMMEDIATELY,"false"));
        if (flowSpecProperties.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)
            && StringUtils.isNotBlank(flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY))) {
          jobConfig.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
        }

        this.scheduledFlowSpecs.put(addedSpec.getUri().toString(), addedSpec);

        if (jobConfig.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
          _log.info("Scheduling flow spec: " + addedSpec);
          scheduleJob(jobConfig, null);
          if (jobConfig.containsKey(ConfigurationKeys.FLOW_RUN_IMMEDIATELY)) {
            _log.info("RunImmediately requested, hence executing FlowSpec: " + addedSpec);
            this.jobExecutor.execute(new NonScheduledJobRunner(jobConfig, null));
          }
        } else {
          _log.info("No FlowSpec schedule found, so running FlowSpec: " + addedSpec);
          this.jobExecutor.execute(new NonScheduledJobRunner(jobConfig, null));
        }
      } catch (JobException je) {
        _log.error("Failed to schedule or run FlowSpec " + addedSpec, je);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    if (this.helixManager.isPresent() && !this.helixManager.get().isConnected()) {
      // Specs in store will be notified when Scheduler is added as listener to FlowCatalog, so ignore
      // .. Specs if in cluster mode and Helix is not yet initialized
      _log.info("System not yet initialized. Skipping Spec Deletion: " + deletedSpecURI);
      return;
    }
    _log.info("Spec deletion detected: " + deletedSpecURI + "/" + deletedSpecVersion);

    if (!isActive && helixManager.isPresent()) {
      _log.info("Scheduler running in slave mode, forward Spec delete via Helix message to master: " + deletedSpecURI);
      HelixUtils.sendUserDefinedMessage(ServiceConfigKeys.HELIX_FLOWSPEC_REMOVE, deletedSpecURI.toString() + ":" +
              deletedSpecVersion, UUID.randomUUID().toString(), InstanceType.CONTROLLER, helixManager.get(), _log);
      return;
    }

    try {
      Spec deletedSpec = this.scheduledFlowSpecs.get(deletedSpecURI.toString());
      if (null != deletedSpec) {
        this.orchestrator.remove(deletedSpec);
        this.scheduledFlowSpecs.remove(deletedSpecURI.toString());
        unscheduleJob(deletedSpecURI.toString());
      } else {
        _log.warn(String.format("Spec with URI: %s was not found in cache. May be it was cleaned, if not please "
                + "clean it manually", deletedSpecURI));
      }
    } catch (JobException e) {
      _log.warn(String.format("Spec with URI: %s was not unscheduled cleaning",
          deletedSpecURI), e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onUpdateSpec(Spec updatedSpec) {
    if (this.helixManager.isPresent() && !this.helixManager.get().isConnected()) {
      // Specs in store will be notified when Scheduler is added as listener to FlowCatalog, so ignore
      // .. Specs if in cluster mode and Helix is not yet initialized
      _log.info("System not yet initialized. Skipping Spec Update: " + updatedSpec);
      return;
    }

    _log.info("Spec changed: " + updatedSpec);

    if (!(updatedSpec instanceof FlowSpec)) {
      return;
    }

    if (!isActive && helixManager.isPresent()) {
      _log.info("Scheduler running in slave mode, forward Spec update via Helix message to master: " + updatedSpec);
      HelixUtils.sendUserDefinedMessage(ServiceConfigKeys.HELIX_FLOWSPEC_UPDATE, updatedSpec.getUri().toString(),
          UUID.randomUUID().toString(), InstanceType.CONTROLLER, helixManager.get(), _log);
      return;
    }

    try {
      onDeleteSpec(updatedSpec.getUri(), updatedSpec.getVersion());
    } catch (Exception e) {
      _log.error("Failed to update Spec: " + updatedSpec, e);
    }
    try {
      onAddSpec(updatedSpec);
    } catch (Exception e) {
      _log.error("Failed to update Spec: " + updatedSpec, e);
    }
  }

  /**
   * A Gobblin job to be scheduled.
   */
  @DisallowConcurrentExecution
  @Slf4j
  public static class GobblinServiceJob extends BaseGobblinJob implements InterruptableJob {
    private static final Logger _log = LoggerFactory.getLogger(GobblinServiceJob.class);

    @Override
    public void executeImpl(JobExecutionContext context)
        throws JobExecutionException {
      _log.info("Starting FlowSpec " + context.getJobDetail().getKey());

      JobDataMap dataMap = context.getJobDetail().getJobDataMap();
      JobScheduler jobScheduler = (JobScheduler) dataMap.get(JOB_SCHEDULER_KEY);
      Properties jobProps = (Properties) dataMap.get(PROPERTIES_KEY);
      JobListener jobListener = (JobListener) dataMap.get(JOB_LISTENER_KEY);

      try {
        jobScheduler.runJob(jobProps, jobListener);
      } catch (Throwable t) {
        throw new JobExecutionException(t);
      }
    }

    @Override
    public void interrupt()
        throws UnableToInterruptJobException {
      log.info("Job was interrupted");
    }
  }

  /**
   * This class is responsible for running non-scheduled jobs.
   */
  class NonScheduledJobRunner implements Runnable {

    private final Properties jobConfig;
    private final JobListener jobListener;

    public NonScheduledJobRunner(Properties jobConfig, JobListener jobListener) {
      this.jobConfig = jobConfig;
      this.jobListener = jobListener;
    }

    @Override
    public void run() {
      try {
        GobblinServiceJobScheduler.this.runJob(this.jobConfig, this.jobListener);
      } catch (JobException je) {
        _log.error("Failed to run job " + this.jobConfig.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
