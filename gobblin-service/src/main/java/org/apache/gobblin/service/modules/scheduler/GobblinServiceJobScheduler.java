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
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.quartz.CronExpression;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.scheduler.BaseGobblinJob;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;

import static org.apache.gobblin.service.ServiceConfigKeys.GOBBLIN_SERVICE_PREFIX;


/**
 * An extension to {@link JobScheduler} that is also a {@link SpecCatalogListener}.
 * {@link GobblinServiceJobScheduler} listens for new / updated {@link FlowSpec} and schedules
 * and runs them via {@link Orchestrator}.
 */
@Alpha
@Singleton
public class GobblinServiceJobScheduler extends JobScheduler implements SpecCatalogListener {

  // Scheduler related configuration
  // A boolean function indicating if current instance will handle DR traffic or not.
  public static final String GOBBLIN_SERVICE_SCHEDULER_DR_NOMINATED = GOBBLIN_SERVICE_PREFIX + "drNominatedInstance";

  protected final Logger _log;

  protected final Optional<FlowCatalog> flowCatalog;
  protected final Optional<HelixManager> helixManager;
  protected final Orchestrator orchestrator;
  protected final Boolean warmStandbyEnabled;
  protected final Optional<UserQuotaManager> quotaManager;
  @Getter
  protected final Map<String, Spec> scheduledFlowSpecs;
  @Getter
  protected final Map<String, Long> lastUpdatedTimeForFlowSpec;
  protected volatile int loadSpecsBatchSize = -1;
  protected int skipSchedulingFlowsAfterNumDays;
  @Getter
  private volatile boolean isActive;
  private String serviceName;
  private volatile Long perSpecGetRateValue = -1L;
  private volatile Long timeToInitializeSchedulerValue = -1L;
  private volatile Long timeToObtainSpecUrisValue = -1L;
  private volatile Long individualGetSpecSpeedValue = -1L;
  private volatile Long eachCompleteAddSpecValue = -1L;
  private volatile Long eachSpecCompilationValue = -1L;
  private volatile Long eachScheduleJobValue = -1L;
  private volatile Long totalGetSpecTimeValue = -1L;
  private volatile Long totalAddSpecTimeValue = -1L;
  private volatile int numJobsScheduledDuringStartupValue = -1;
  private final ContextAwareGauge getSpecsPerSpecRateNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_GET_SPECS_DURING_STARTUP_PER_SPEC_RATE_NANOS, () -> this.perSpecGetRateValue);
  private final ContextAwareGauge batchSize = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_LOAD_SPECS_BATCH_SIZE, () -> this.loadSpecsBatchSize);
  private final ContextAwareGauge timeToInitalizeSchedulerNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_TIME_TO_INITIALIZE_SCHEDULER_NANOS, () -> this.timeToInitializeSchedulerValue);
  private final ContextAwareGauge timeToObtainSpecUrisNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_TIME_TO_OBTAIN_SPEC_URIS_NANOS, () -> timeToObtainSpecUrisValue);
  private final ContextAwareGauge individualGetSpecSpeedNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_INDIVIDUAL_GET_SPEC_SPEED_NANOS, () -> individualGetSpecSpeedValue);
  private final ContextAwareGauge eachCompleteAddSpecNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_EACH_COMPLETE_ADD_SPEC_NANOS, () -> eachCompleteAddSpecValue);
  private final ContextAwareGauge eachSpecCompilationNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_EACH_SPEC_COMPILATION_NANOS, () -> eachSpecCompilationValue);
  private final ContextAwareGauge eachScheduleJobNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_EACH_SCHEDULE_JOB_NANOS, () -> eachScheduleJobValue);
  private final ContextAwareGauge totalGetSpecTimeNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_TOTAL_GET_SPEC_TIME_NANOS, () -> totalGetSpecTimeValue);
  private final ContextAwareGauge totalAddSpecTimeNanos = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_TOTAL_ADD_SPEC_TIME_NANOS, () -> totalAddSpecTimeValue);
  private final ContextAwareGauge numJobsScheduledDuringStartup = metricContext.newContextAwareGauge(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_NUM_JOBS_SCHEDULED_DURING_STARTUP, () -> numJobsScheduledDuringStartupValue);
  private static final MetricContext metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(),
      GobblinServiceJobScheduler.class);
  private static final ContextAwareMeter scheduledFlows = metricContext.contextAwareMeter(ServiceMetricNames.SCHEDULED_FLOW_METER);
  private static final ContextAwareMeter nonScheduledFlows = metricContext.contextAwareMeter(ServiceMetricNames.NON_SCHEDULED_FLOW_METER);

  /**
   * If current instances is nominated as a handler for DR traffic from down GaaS-Instance.
   * Note this is, currently, different from leadership change/fail-over handling, where the traffic could come
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

  @Inject
  public GobblinServiceJobScheduler(@Named(InjectionNames.SERVICE_NAME) String serviceName,
      Config config,
      Optional<HelixManager> helixManager, Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog,
      Orchestrator orchestrator, SchedulerService schedulerService, Optional<UserQuotaManager> quotaManager, Optional<Logger> log,
      @Named(InjectionNames.WARM_STANDBY_ENABLED) boolean warmStandbyEnabled) throws Exception {
    super(ConfigUtils.configToProperties(config), schedulerService);

    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.serviceName = serviceName;
    this.flowCatalog = flowCatalog;
    this.helixManager = helixManager;
    this.orchestrator = orchestrator;
    this.scheduledFlowSpecs = Maps.newHashMap();
    this.lastUpdatedTimeForFlowSpec = Maps.newHashMap();
    this.loadSpecsBatchSize = Integer.parseInt(ConfigUtils.configToProperties(config).getProperty(ConfigurationKeys.LOAD_SPEC_BATCH_SIZE, String.valueOf(ConfigurationKeys.DEFAULT_LOAD_SPEC_BATCH_SIZE)));
    this.skipSchedulingFlowsAfterNumDays = Integer.parseInt(ConfigUtils.configToProperties(config).getProperty(ConfigurationKeys.SKIP_SCHEDULING_FLOWS_AFTER_NUM_DAYS, String.valueOf(ConfigurationKeys.DEFAULT_NUM_DAYS_TO_SKIP_AFTER)));
    this.isNominatedDRHandler = config.hasPath(GOBBLIN_SERVICE_SCHEDULER_DR_NOMINATED)
        && config.hasPath(GOBBLIN_SERVICE_SCHEDULER_DR_NOMINATED);
    this.warmStandbyEnabled = warmStandbyEnabled;
    this.quotaManager = quotaManager;
    // Check that these metrics do not exist before adding, mainly for testing purpose which creates multiple instances
    // of the scheduler. If one metric exists, then the others should as well.
    MetricFilter filter = MetricFilter.contains(RuntimeMetrics.GOBBLIN_JOB_SCHEDULER_GET_SPECS_DURING_STARTUP_PER_SPEC_RATE_NANOS);
    if (metricContext.getGauges(filter).isEmpty()) {
      metricContext.register(this.getSpecsPerSpecRateNanos);
      metricContext.register(this.batchSize);
      metricContext.register(this.timeToInitalizeSchedulerNanos);
      metricContext.register(this.timeToObtainSpecUrisNanos);
      metricContext.register(this.individualGetSpecSpeedNanos);
      metricContext.register(this.eachCompleteAddSpecNanos);
      metricContext.register(this.eachSpecCompilationNanos);
      metricContext.register(this.eachScheduleJobNanos);
      metricContext.register(this.totalGetSpecTimeNanos);
      metricContext.register(this.totalAddSpecTimeNanos);
      metricContext.register(this.numJobsScheduledDuringStartup);
    }
  }

  public GobblinServiceJobScheduler(String serviceName, Config config, FlowStatusGenerator flowStatusGenerator,
      Optional<HelixManager> helixManager,
      Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog, Optional<DagManager> dagManager, Optional<UserQuotaManager> quotaManager,
      SchedulerService schedulerService,  Optional<Logger> log, boolean warmStandbyEnabled) throws Exception {
    this(serviceName, config, helixManager, flowCatalog, topologyCatalog,
        new Orchestrator(config, flowStatusGenerator, topologyCatalog, dagManager, log), schedulerService, quotaManager, log, warmStandbyEnabled);
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
            // Ensure compiler is healthy before attempting to schedule flows
            try {
              GobblinServiceJobScheduler.this.orchestrator.getSpecCompiler().awaitHealthy();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            scheduleSpecsFromCatalog();
          }
        });
        scheduleSpec.start();
      }
    } else {
      // Since we are going to change status to isActive=false, unschedule all flows
      try {
        this.scheduledFlowSpecs.clear();
        unscheduleAllJobs();
      } catch (SchedulerException e) {
        _log.error(String.format("Not all jobs were unscheduled"), e);
        // We want to avoid duplicate flow execution, so fail loudly
        throw new RuntimeException(e);
      }
      // Need to set active=false at the end; otherwise in the onDeleteSpec(), node will forward specs to active node, which is itself.
      this.isActive = isActive;
    }
  }

  /** Return true if a spec should be scheduled and if it is, modify the spec of an adhoc flow before adding to
   *  scheduler. Return false otherwise. */
  private boolean addSpecHelperMethod(Spec spec) {
    // Adhoc flows will not have any job schedule key, but we should schedule them
    if (spec instanceof FlowSpec) {
      FlowSpec flowSpec = (FlowSpec) spec;
      if (!flowSpec.getConfig().hasPath(ConfigurationKeys.JOB_SCHEDULE_KEY) || isWithinRange(
          flowSpec.getConfig().getString(ConfigurationKeys.JOB_SCHEDULE_KEY), this.skipSchedulingFlowsAfterNumDays)) {
        // Disable FLOW_RUN_IMMEDIATELY on service startup or leadership change if the property is set to true
        if (PropertiesUtils.getPropAsBoolean(flowSpec.getConfigAsProperties(), ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false")) {
          Spec modifiedSpec = disableFlowRunImmediatelyOnStart((FlowSpec) spec);
          onAddSpec(modifiedSpec);
        } else {
          onAddSpec(spec);
        }
        return true;
      }
    }else {
      _log.debug("Not scheduling spec {} during startup as next job to schedule is outside of threshold.", spec);
    }
    return false;
  }

  /**
   * Returns true if next run for the given cron schedule is sooner than the threshold to skip scheduling after, false
   * otherwise. If the cron expression cannot be parsed and the next run cannot be calculated returns true to schedule.
   * @param cronExpression
   * @return num days until next run, max integer in the case it cannot be calculated
   */
  @VisibleForTesting
  public static boolean isWithinRange(String cronExpression, int maxNumDaysToScheduleWithin) {
    CronExpression cron = null;
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    double numMillisInADay = 86400000;
    try {
      cron = new CronExpression(cronExpression);
      cron.setTimeZone(TimeZone.getTimeZone("UTC"));
      Date nextValidTimeAfter = cron.getNextValidTimeAfter(new Date());
      cal.setTime(nextValidTimeAfter);
      long diff = cal.getTimeInMillis() - System.currentTimeMillis();
      return (int) Math.round(diff / numMillisInADay) < maxNumDaysToScheduleWithin;
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * Load all {@link FlowSpec}s from {@link FlowCatalog} as one of the initialization step,
   * and make schedulers be aware of that.
   *
   * If it is newly brought up as the DR handler, will load additional FlowSpecs and handle transition properly.
   */
  private void scheduleSpecsFromCatalog() {
    int numSpecs = this.flowCatalog.get().getSize();
    int actualNumFlowsScheduled = 0;
    _log.info("Scheduling specs from catalog: {} flows in the catalog, will skip scheduling flows with next run after "
        + "{} days", numSpecs, this.skipSchedulingFlowsAfterNumDays);
    long startTime = System.nanoTime();
    long totalGetTime = 0;
    long totalAddSpecTime = 0;
    Iterator<URI> uriIterator;
    HashSet<URI> urisLeftToSchedule = new HashSet<>();
    try {
      uriIterator = this.flowCatalog.get().getSpecURIs();
      while (uriIterator.hasNext()) {
        urisLeftToSchedule.add(uriIterator.next());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.timeToObtainSpecUrisValue = System.nanoTime() - startTime;

    try {
      // If current instances nominated as DR handler, will take additional URIS from FlowCatalog.
      if (isNominatedDRHandler) {
        // Synchronously cleaning the execution state for DR-applicable FlowSpecs
        // before rescheduling the again in nominated DR-Hanlder.
        Iterator<URI> drUris = this.flowCatalog.get().getSpecURISWithTag(DR_FILTER_TAG);
        clearRunningFlowState(drUris);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to get Spec URIs with tag to clear running flow state", e);
    }

    int startOffset = 0;
    long batchGetStartTime;
    long batchGetEndTime;

    while (startOffset < numSpecs) {
      batchGetStartTime  = System.nanoTime();
      Collection<Spec> batchOfSpecs = this.flowCatalog.get().getSpecsPaginated(startOffset, this.loadSpecsBatchSize);
      Iterator<Spec> batchOfSpecsIterator = batchOfSpecs.iterator();
      batchGetEndTime = System.nanoTime();

      while (batchOfSpecsIterator.hasNext()) {
        Spec spec = batchOfSpecsIterator.next();
        try {
          if (addSpecHelperMethod(spec)) {
            totalAddSpecTime += this.eachCompleteAddSpecValue; // this is updated by each call to onAddSpec
            actualNumFlowsScheduled += 1;
          }
        } catch (Exception e) {
          // If there is an uncaught error thrown during compilation, log it and continue adding flows
          _log.error("Could not schedule spec {} from flowCatalog due to ", spec, e);
        }
        urisLeftToSchedule.remove(spec.getUri());
      }
      startOffset += this.loadSpecsBatchSize;
      totalGetTime += batchGetEndTime - batchGetStartTime;
      // Don't skew the average get spec time value with the last batch that may be very small
      if (startOffset == 0 || batchOfSpecs.size() >=  Math.round(0.75 * this.loadSpecsBatchSize)) {
        perSpecGetRateValue = (batchGetEndTime - batchGetStartTime) / batchOfSpecs.size();
      }
    }

    // Ensure we did not miss any specs due to ordering changing (deletions/insertions) while loading
    Iterator<URI> urisLeft = urisLeftToSchedule.iterator();
    long individualGetSpecStartTime;
    while (urisLeft.hasNext()) {
        URI uri = urisLeft.next();
        try {
          individualGetSpecStartTime = System.nanoTime();
          Spec spec = this.flowCatalog.get().getSpecWrapper(uri);
          this.individualGetSpecSpeedValue = System.nanoTime() - individualGetSpecStartTime;
          totalGetTime += this.individualGetSpecSpeedValue;
          if (addSpecHelperMethod(spec)) {
            totalAddSpecTime += this.eachCompleteAddSpecValue; // this is updated by each call to onAddSpec
            actualNumFlowsScheduled += 1;
          }
        } catch (Exception e) {
          // If there is an uncaught error thrown during compilation, log it and continue adding flows
          _log.error("Could not schedule spec uri {} from flowCatalog due to {}", uri, e);
        }
    }
    // Reset value after its last value to get an accurate reading
    this.perSpecGetRateValue = -1L;
    this.individualGetSpecSpeedValue = -1L;

    this.totalGetSpecTimeValue = totalGetTime;
    this.totalAddSpecTimeValue = totalAddSpecTime;
    this.numJobsScheduledDuringStartupValue = actualNumFlowsScheduled;
    this.flowCatalog.get().getMetrics().updateGetSpecTime(startTime);
    this.timeToInitializeSchedulerValue = System.nanoTime() - startTime;
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
    return new FlowSpec(spec.getUri(), spec.getVersion(), spec.getDescription(), config, properties,
        spec.getTemplateURIs(), spec.getChildSpecs());
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

  /**
   *
   * @param addedSpec spec to be added
   * @return add spec response, which contains <code>null</code> if there is an error
   */
  @Override
  public AddSpecResponse onAddSpec(Spec addedSpec) {
    long startTime = System.nanoTime();
    if (this.helixManager.isPresent() && !this.helixManager.get().isConnected()) {
      // Specs in store will be notified when Scheduler is added as listener to FlowCatalog, so ignore
      // .. Specs if in cluster mode and Helix is not yet initialized
      _log.info("System not yet initialized. Skipping Spec Addition: " + addedSpec);
      return null;
    }

    _log.info("New Flow Spec detected: " + addedSpec);

    if (!(addedSpec instanceof FlowSpec)) {
      return null;
    }

    FlowSpec flowSpec = (FlowSpec) addedSpec;
    URI flowSpecUri = flowSpec.getUri();
    Properties jobConfig = createJobConfig(flowSpec);
    boolean isExplain = flowSpec.isExplain();
    String response = null;

    long compileStartTime = System.nanoTime();
    // always try to compile the flow to verify if it is compilable
    Dag<JobExecutionPlan> dag = this.orchestrator.getSpecCompiler().compileFlow(flowSpec);
    this.eachSpecCompilationValue = System.nanoTime() - compileStartTime;
    // If dag is null then a compilation error has occurred
    if (dag != null && !dag.isEmpty()) {
      response = dag.toString();
    }

    boolean compileSuccess = FlowCatalog.isCompileSuccessful(response);

    if (isExplain || !compileSuccess || !this.isActive) {
      // todo: in case of a scheduled job, we should also check if the job schedule is a valid cron schedule
      //  so it can be scheduled
      _log.info("Ignoring the spec {}. isExplain: {}, compileSuccess: {}, master: {}",
          addedSpec, isExplain, compileSuccess, this.isActive);
      return new AddSpecResponse<>(response);
    }
    // Check quota limits against adhoc flows before saving the schedule
    // In warm standby mode, this quota check will happen on restli API layer when we accept the flow
    if (!this.warmStandbyEnabled && !jobConfig.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
      // This block should be reachable only for the execution for the adhoc flows
      // For flow that has scheduler but run-immediately set to be true, we won't check the quota as we will use a different execution id later
      if (quotaManager.isPresent()) {
        // QuotaManager has idempotent checks for a dagNode, so this check won't double add quotas for a flow in the DagManager
        try {
          quotaManager.get().checkQuota(dag.getStartNodes());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Compare the modification timestamp of the spec being added if the scheduler is being initialized, ideally we
    // don't even want to do the same update twice as it will kill the existing flow and reschedule it unnecessarily
    Long modificationTime = Long.valueOf(flowSpec.getConfigAsProperties().getProperty(FlowSpec.MODIFICATION_TIME_KEY, "0"));
    String uriString = flowSpec.getUri().toString();
    Boolean isRunImmediately = PropertiesUtils.getPropAsBoolean(jobConfig, ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false");
    // If the modification time is 0 (which means the original API was used to retrieve spec or warm standby mode is not
    // enabled), spec not in scheduler, or have a modification time associated with it assume it's the most recent
    if (modificationTime != 0L && this.scheduledFlowSpecs.containsKey(uriString)
        && this.lastUpdatedTimeForFlowSpec.containsKey(uriString)) {
      // For run-immediately flows with a schedule the modified_time would remain the same
      if (this.lastUpdatedTimeForFlowSpec.get(uriString).compareTo(modificationTime) > 0
          || (this.lastUpdatedTimeForFlowSpec.get(uriString).equals(modificationTime) && !isRunImmediately)) {
        _log.warn("Ignoring the spec {} modified at time {} because we have a more updated version from time {}",
            addedSpec, modificationTime,this.lastUpdatedTimeForFlowSpec.get(uriString));
        this.eachCompleteAddSpecValue = System.nanoTime() - startTime;
        return new AddSpecResponse(response);
      }
    }

    // todo : we should probably not schedule a flow if it is a runOnce flow
    this.scheduledFlowSpecs.put(flowSpecUri.toString(), addedSpec);
    this.lastUpdatedTimeForFlowSpec.put(flowSpecUri.toString(), modificationTime);

    if (jobConfig.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
      _log.info("{} Scheduling flow spec: {} ", this.serviceName, addedSpec);
      try {
        long scheduleStartTime = System.nanoTime();
        scheduleJob(jobConfig, null);
        this.eachScheduleJobValue = System.nanoTime() - scheduleStartTime;
      } catch (JobException je) {
        _log.error("{} Failed to schedule or run FlowSpec {}", serviceName, addedSpec, je);
        this.scheduledFlowSpecs.remove(addedSpec.getUri().toString());
        this.lastUpdatedTimeForFlowSpec.remove(flowSpecUri.toString());
        this.eachCompleteAddSpecValue = System.nanoTime() - startTime;
        return null;
      }
      if (PropertiesUtils.getPropAsBoolean(jobConfig, ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false")) {
        _log.info("RunImmediately requested, hence executing FlowSpec: " + addedSpec);
        this.jobExecutor.execute(new NonScheduledJobRunner(flowSpecUri, false, jobConfig, null));
      }
    } else {
      _log.info("No FlowSpec schedule found, so running FlowSpec: " + addedSpec);
      this.jobExecutor.execute(new NonScheduledJobRunner(flowSpecUri, true, jobConfig, null));
    }

    this.eachCompleteAddSpecValue = System.nanoTime() - startTime;
    return new AddSpecResponse<>(response);
  }

  /**
   * Remove a flowSpec from schedule
   * Unlike onDeleteSpec, we want to avoid deleting the flowSpec on the executor
   * and we still want to unschedule if we cannot connect to zookeeper as the current node cannot be the master
   * @param specURI
   * @param specVersion
   */
  private void unscheduleSpec(URI specURI, String specVersion) throws JobException {
    if (this.scheduledFlowSpecs.containsKey(specURI.toString())) {
      _log.info("Unscheduling flowSpec " + specURI + "/" + specVersion);
      this.scheduledFlowSpecs.remove(specURI.toString());
      this.lastUpdatedTimeForFlowSpec.remove(specURI.toString());
      unscheduleJob(specURI.toString());
    } else {
      throw new JobException(String.format(
          "Spec with URI: %s was not found in cache. May be it was cleaned, if not please clean it manually",
          specURI));
    }
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

    if (!this.isActive) {
      _log.info("Skipping deletion of this spec {}/{} for non-leader host", deletedSpecURI, deletedSpecVersion);
      return;
    }

    try {
      Spec deletedSpec = this.scheduledFlowSpecs.get(deletedSpecURI.toString());
      unscheduleSpec(deletedSpecURI, deletedSpecVersion);
      this.orchestrator.remove(deletedSpec, headers);
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
      onAddSpec(updatedSpec);
    } catch (Exception e) {
      _log.error("Failed to update Spec: " + updatedSpec, e);
    }
  }

  private Properties createJobConfig(FlowSpec flowSpec) {
    Properties jobConfig = new Properties();
    Properties flowSpecProperties = flowSpec.getConfigAsProperties();

    jobConfig.putAll(this.properties);
    jobConfig.setProperty(ConfigurationKeys.JOB_NAME_KEY, flowSpec.getUri().toString());
    jobConfig.setProperty(ConfigurationKeys.JOB_GROUP_KEY,
        flowSpec.getConfig().getValue(ConfigurationKeys.FLOW_GROUP_KEY).toString());
    jobConfig.setProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY,
        ConfigUtils.getString((flowSpec).getConfig(), ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false"));

    // todo : we should check if the job schedule is a valid cron schedule
    if (flowSpecProperties.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY) && StringUtils.isNotBlank(
        flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY))) {
      jobConfig.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY,
          flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
    }

    return jobConfig;
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
      } finally {
        scheduledFlows.mark();
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
          Object syncObject = GobblinServiceJobScheduler.this.flowCatalog.get().getSyncObject(specUri.toString());
          if (syncObject != null) {
            // if the sync object does not exist, this job must be set to run due to job submission at service restart
            synchronized (syncObject) {
              while (!GobblinServiceJobScheduler.this.flowCatalog.get().exists(specUri)) {
                syncObject.wait();
              }
            }
          }
          GobblinServiceJobScheduler.this.flowCatalog.get().remove(specUri, new Properties(), false);
          GobblinServiceJobScheduler.this.scheduledFlowSpecs.remove(specUri.toString());
          GobblinServiceJobScheduler.this.lastUpdatedTimeForFlowSpec.remove(specUri.toString());
        }
      } catch (JobException je) {
        _log.error("Failed to run job " + this.jobConfig.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      } catch (InterruptedException e) {
      _log.error("Failed to delete the spec " + specUri, e);
      } finally {
        nonScheduledFlows.mark();
      }
    }
  }
}
