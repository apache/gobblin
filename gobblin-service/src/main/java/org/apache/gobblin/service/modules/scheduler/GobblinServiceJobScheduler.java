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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.scheduler.BaseGobblinJob;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;

import static org.apache.gobblin.service.ServiceConfigKeys.GOBBLIN_SERVICE_PREFIX;


/**
 * An extension to {@link JobScheduler} that is also a {@link SpecCatalogListener}.
 * {@link GobblinServiceJobScheduler} listens for new / updated {@link FlowSpec} and schedules
 * and runs them via {@link Orchestrator}.
 */
@Alpha
public class GobblinServiceJobScheduler extends JobScheduler implements SpecCatalogListener {

  // Scheduler related configuration
  // A boolean function indicating if current instance will handle DR traffic or not.
  public static final String GOBBLIN_SERVICE_SCHEDULER_DR_NOMINATED = GOBBLIN_SERVICE_PREFIX + "drNominatedInstance";

  protected final Logger _log;

  protected final Optional<FlowCatalog> flowCatalog;
  protected final Optional<HelixManager> helixManager;
  protected final Orchestrator orchestrator;
  @Getter
  protected final Map<String, Spec> scheduledFlowSpecs;
  @Getter
  private volatile boolean isActive;
  private String serviceName;

  /**
   * If current instances is nominated as a handler for DR traffic from down GaaS-Instance.
   * Note this is, currently, different from leadership change/fail-over handling, where the traffice could come
   * from GaaS instance out of current GaaS Cluster:
   * e.g. There are multi-datacenter deployment of GaaS Cluster. Intra-datacenter fail-over could be handled by
   * leadership change mechanism, while inter-datacenter fail-over would be handled by DR handling mechanism.
   */
  private boolean isNominatedDRHandler;

  /**
   * Use this to tag all DR-applicable FlowSpec entries in {@link org.apache.gobblin.runtime.api.SpecStore}
   * so only they would be loaded during DR handling.
   */
  public static final String DR_FILTER_TAG = "dr";

  public GobblinServiceJobScheduler(String serviceName, Config config, Optional<HelixManager> helixManager,
      Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog, Orchestrator orchestrator,
      SchedulerService schedulerService, Optional<Logger> log) throws Exception {
    super(ConfigUtils.configToProperties(config), schedulerService);

    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.serviceName = serviceName;
    this.flowCatalog = flowCatalog;
    this.helixManager = helixManager;
    this.orchestrator = orchestrator;
    this.scheduledFlowSpecs = Maps.newHashMap();
    this.isNominatedDRHandler = config.hasPath(GOBBLIN_SERVICE_SCHEDULER_DR_NOMINATED)
        && config.hasPath(GOBBLIN_SERVICE_SCHEDULER_DR_NOMINATED);
  }

  public GobblinServiceJobScheduler(String serviceName, Config config, Optional<HelixManager> helixManager,
      Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog, Optional<DagManager> dagManager,
      SchedulerService schedulerService, Optional<Logger> log) throws Exception {
    this(serviceName, config, helixManager, flowCatalog, topologyCatalog,
        new Orchestrator(config, topologyCatalog, dagManager, log), schedulerService, log);
  }

  public synchronized void setActive(boolean isActive) {
    if (this.isActive == isActive) {
      // No-op if already in correct state
      return;
    }

    // Since we are going to change status to isActive=true, schedule all flows
    if (isActive) {
      // Need to set active=true first; otherwise in the onAddSpec(), node will forward specs to active node, which is itself.
      this.isActive = isActive;

      if (this.flowCatalog.isPresent()) {
        // Load spec asynchronously and make scheduler be aware of that.
        Thread scheduleSpec = new Thread(new Runnable() {
          @Override
          public void run() {
            scheduleSpecsFromCatalog();
          }
        });
        scheduleSpec.start();
      }
    } else {
      // Since we are going to change status to isActive=false, unschedule all flows
      for (Spec spec : this.scheduledFlowSpecs.values()) {
        onDeleteSpec(spec.getUri(), spec.getVersion());
      }
      // Need to set active=false at the end; otherwise in the onDeleteSpec(), node will forward specs to active node, which is itself.
      this.isActive = isActive;
    }
  }

  /**
   * Load all {@link FlowSpec}s from {@link FlowCatalog} as one of the initialization step,
   * and make schedulers be aware of that.
   *
   * If it is newly brought up as the DR handler, will load additional FlowSpecs and handle transition properly.
   */
  private void scheduleSpecsFromCatalog() {
    Iterator<URI> specUris = null;
    long startTime = System.currentTimeMillis();

    try {
      specUris = this.flowCatalog.get().getSpecURIs();

      // If current instances nominated as DR handler, will take additional URIS from FlowCatalog.
      if (isNominatedDRHandler) {
        // Synchronously cleaning the execution state for DR-applicable FlowSpecs
        // before rescheduling the again in nominated DR-Hanlder.
        Iterator<URI> drUris = this.flowCatalog.get().getSpecURISWithTag(DR_FILTER_TAG);
        clearRunningFlowState(drUris);
      }

    } catch (IOException e) {
      throw new RuntimeException("Failed to get the iterator of all Spec URIS", e);
    }

    try {
      while (specUris.hasNext()) {
        Spec spec = this.flowCatalog.get().getSpecWrapper(specUris.next());
        //Disable FLOW_RUN_IMMEDIATELY on service startup or leadership change
        if (spec instanceof FlowSpec) {
          Spec modifiedSpec = disableFlowRunImmediatelyOnStart((FlowSpec) spec);
          onAddSpec(modifiedSpec);
        } else {
          onAddSpec(spec);
        }
      }
    } finally {
      this.flowCatalog.get().getMetrics().updateGetSpecTime(startTime);
    }
  }

  /**
   * In DR-mode, the running {@link FlowSpec} will all be cancelled and rescheduled.
   * We will need to make sure that running {@link FlowSpec}s' state are cleared, and corresponding running jobs are
   * killed before rescheduling them.
   * @param drUris The uris that applicable for DR discovered from FlowCatalog.
   */
  private void clearRunningFlowState(Iterator<URI> drUris) {
    while (drUris.hasNext()) {
      // TODO: Instead of simply call onDeleteSpec, a callback when FlowSpec is deleted from FlowCatalog, should also kill Azkaban Flow from AzkabanSpecProducer.
      onDeleteSpec(drUris.next(), FlowSpec.Builder.DEFAULT_VERSION);
    }
  }

  @VisibleForTesting
  protected static Spec disableFlowRunImmediatelyOnStart(FlowSpec spec) {
    Properties properties = spec.getConfigAsProperties();
    properties.setProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false");
    Config config = ConfigFactory.parseProperties(properties);
    FlowSpec flowSpec = new FlowSpec(spec.getUri(), spec.getVersion(), spec.getDescription(), config, properties,
        spec.getTemplateURIs(), spec.getChildSpecs());
    return flowSpec;
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
  public AddSpecResponse onAddSpec(Spec addedSpec) {
    if (this.helixManager.isPresent() && !this.helixManager.get().isConnected()) {
      // Specs in store will be notified when Scheduler is added as listener to FlowCatalog, so ignore
      // .. Specs if in cluster mode and Helix is not yet initialized
      _log.info("System not yet initialized. Skipping Spec Addition: " + addedSpec);
      return null;
    }

    _log.info("New Flow Spec detected: " + addedSpec);

    if (addedSpec instanceof FlowSpec) {
      try {
        FlowSpec flowSpec = (FlowSpec) addedSpec;
        Properties jobConfig = new Properties();
        Properties flowSpecProperties = ((FlowSpec) addedSpec).getConfigAsProperties();
        jobConfig.putAll(this.properties);
        jobConfig.setProperty(ConfigurationKeys.JOB_NAME_KEY, addedSpec.getUri().toString());
        jobConfig.setProperty(ConfigurationKeys.JOB_GROUP_KEY,
            flowSpec.getConfig().getValue(ConfigurationKeys.FLOW_GROUP_KEY).toString());
        jobConfig.setProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY,
            ConfigUtils.getString((flowSpec).getConfig(), ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false"));
        if (flowSpecProperties.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY) && StringUtils.isNotBlank(
            flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY))) {
          jobConfig.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY,
              flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
        }
        boolean isExplain = ConfigUtils.getBoolean(flowSpec.getConfig(), ConfigurationKeys.FLOW_EXPLAIN_KEY, false);
        String response = null;
        if (!isExplain) {
          this.scheduledFlowSpecs.put(addedSpec.getUri().toString(), addedSpec);

          if (jobConfig.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
            _log.info("{} Scheduling flow spec: {} ", this.serviceName, addedSpec);
            scheduleJob(jobConfig, null);
            if (PropertiesUtils.getPropAsBoolean(jobConfig, ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false")) {
              _log.info("RunImmediately requested, hence executing FlowSpec: " + addedSpec);
              this.jobExecutor.execute(new NonScheduledJobRunner(flowSpec.getUri(), false, jobConfig, null));
            }
          } else {
            _log.info("No FlowSpec schedule found, so running FlowSpec: " + addedSpec);
            this.jobExecutor.execute(new NonScheduledJobRunner(flowSpec.getUri(), true, jobConfig, null));
          }
        } else {
          //Return a compiled flow.
          Dag<JobExecutionPlan> dag = this.orchestrator.getSpecCompiler().compileFlow(flowSpec);
          if (dag != null && !dag.isEmpty()) {
            response = dag.toString();
          } else if (!flowSpec.getCompilationErrors().isEmpty()) {
            response = Arrays.toString(flowSpec.getCompilationErrors().toArray());
          }
          _log.info("{} Skipping adding flow spec: {}, since it is an EXPLAIN request", this.serviceName, addedSpec);

          if (this.flowCatalog.isPresent()) {
            _log.debug("{} Removing flow spec from FlowCatalog: {}", this.serviceName, flowSpec);
            this.flowCatalog.get().remove(flowSpec.getUri(), new Properties(), false);
          }
        }
        return new AddSpecResponse(response);
      } catch (JobException je) {
        _log.error("{} Failed to schedule or run FlowSpec {}", serviceName, addedSpec, je);
      }
    }
    return null;
  }

  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    onDeleteSpec(deletedSpecURI, deletedSpecVersion, new Properties());
  }

  /** {@inheritDoc} */
  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion, Properties headers) {
    if (this.helixManager.isPresent() && !this.helixManager.get().isConnected()) {
      // Specs in store will be notified when Scheduler is added as listener to FlowCatalog, so ignore
      // .. Specs if in cluster mode and Helix is not yet initialized
      _log.info("System not yet initialized. Skipping Spec Deletion: " + deletedSpecURI);
      return;
    }
    _log.info("Spec deletion detected: " + deletedSpecURI + "/" + deletedSpecVersion);

    try {
      Spec deletedSpec = this.scheduledFlowSpecs.get(deletedSpecURI.toString());
      if (null != deletedSpec) {
        this.orchestrator.remove(deletedSpec, headers);
        this.scheduledFlowSpecs.remove(deletedSpecURI.toString());
        unscheduleJob(deletedSpecURI.toString());
      } else {
        _log.warn(String.format(
            "Spec with URI: %s was not found in cache. May be it was cleaned, if not please " + "clean it manually",
            deletedSpecURI));
      }
    } catch (JobException | IOException e) {
      _log.warn(String.format("Spec with URI: %s was not unscheduled cleaning", deletedSpecURI), e);
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
    public void executeImpl(JobExecutionContext context) throws JobExecutionException {
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
    public void interrupt() throws UnableToInterruptJobException {
      log.info("Job was interrupted");
    }
  }

  /**
   * This class is responsible for running non-scheduled jobs.
   */
  class NonScheduledJobRunner implements Runnable {
    private final URI specUri;
    private final Properties jobConfig;
    private final JobListener jobListener;
    private final boolean removeSpec;

    public NonScheduledJobRunner(URI uri, boolean removeSpec, Properties jobConfig, JobListener jobListener) {
      this.specUri = uri;
      this.jobConfig = jobConfig;
      this.jobListener = jobListener;
      this.removeSpec = removeSpec;
    }

    @Override
    public void run() {
      try {
        GobblinServiceJobScheduler.this.runJob(this.jobConfig, this.jobListener);
        if (flowCatalog.isPresent() && removeSpec) {
          GobblinServiceJobScheduler.this.flowCatalog.get().remove(specUri, new Properties(), false);
        }
      } catch (JobException je) {
        _log.error("Failed to run job " + this.jobConfig.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
