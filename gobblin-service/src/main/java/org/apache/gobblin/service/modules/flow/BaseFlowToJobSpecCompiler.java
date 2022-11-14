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

package org.apache.gobblin.service.modules.flow;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.job_catalog.FSJobCatalog;
import org.apache.gobblin.runtime.job_spec.ResolvedJobSpec;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;


// Provide base implementation for constructing multi-hops route.
@Alpha
public abstract class BaseFlowToJobSpecCompiler implements SpecCompiler {

  // Since {@link SpecCompiler} is an {@link SpecCatalogListener}, it is expected that any Spec change should be reflected
  // to these data structures.
  @Getter
  @Setter
  protected final Map<URI, TopologySpec> topologySpecMap;

  protected final Config config;
  protected final Logger log;
  protected final Optional<FSJobCatalog> templateCatalog;

  protected final MetricContext metricContext;
  @Getter
  protected Optional<Meter> flowCompilationSuccessFulMeter;
  @Getter
  protected Optional<Meter> flowCompilationFailedMeter;
  @Getter
  protected Optional<Timer> flowCompilationTimer;
  @Getter
  protected Optional<Timer> dataAuthorizationTimer;
  @Getter
  @Setter
  protected boolean active;

  private boolean warmStandbyEnabled;

  private Optional<UserQuotaManager> userQuotaManager;

  public BaseFlowToJobSpecCompiler(Config config){
    this(config,true);
  }

  public BaseFlowToJobSpecCompiler(Config config, boolean instrumentationEnabled){
    this(config, Optional.<Logger>absent(),  true);
  }

  public BaseFlowToJobSpecCompiler(Config config, Optional<Logger> log){
    this(config, log,true);
  }

  public BaseFlowToJobSpecCompiler(Config config, Optional<Logger> log, boolean instrumentationEnabled){
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    if (instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), IdentityFlowToJobSpecCompiler.class);
      this.flowCompilationSuccessFulMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_COMPILATION_SUCCESSFUL_METER));
      this.flowCompilationFailedMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_COMPILATION_FAILED_METER));
      this.flowCompilationTimer = Optional.<Timer>of(this.metricContext.timer(ServiceMetricNames.FLOW_COMPILATION_TIMER));
      this.dataAuthorizationTimer = Optional.<Timer>of(this.metricContext.timer(ServiceMetricNames.DATA_AUTHORIZATION_TIMER));
    }
    else {
      this.metricContext = null;
      this.flowCompilationSuccessFulMeter = Optional.absent();
      this.flowCompilationFailedMeter = Optional.absent();
      this.flowCompilationTimer = Optional.absent();
      this.dataAuthorizationTimer = Optional.absent();
    }

    this.warmStandbyEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_WARM_STANDBY_ENABLED_KEY, false);
    if (this.warmStandbyEnabled) {
      userQuotaManager = Optional.of(GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
          ConfigUtils.getString(config, ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER), config));
    } else {
      userQuotaManager = Optional.absent();
    }

    this.topologySpecMap = Maps.newConcurrentMap();
    this.config = config;

    /***
     * ETL-5996
     * For multi-tenancy, the following needs to be added:
     * 1. Change singular templateCatalog to Map<URI, JobCatalogWithTemplates> to support multiple templateCatalogs
     * 2. Pick templateCatalog from JobCatalogWithTemplates based on URI, and try to resolve JobSpec using that
     */
    try {
      if (this.config.hasPath(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY)
          && StringUtils.isNotBlank(this.config.getString(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY))) {
        Config templateCatalogCfg = config
            .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
                this.config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
        this.templateCatalog = Optional.of(new FSJobCatalog(templateCatalogCfg));
      } else {
        this.templateCatalog = Optional.absent();
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize FlowCompiler because of "
          + "TemplateCatalog initialization failure", e);
    }
  }

  @Override
  public void awaitHealthy() throws InterruptedException {
    //Do nothing
    return;
  }

  private synchronized  AddSpecResponse onAddTopologySpec(TopologySpec spec) {
    log.info("Loading topology {}", spec.toLongString());
    for (Map.Entry entry : spec.getConfigAsProperties().entrySet()) {
      log.info("topo: {} --> {}", entry.getKey(), entry.getValue());
    }

    topologySpecMap.put(spec.getUri(), spec);
    return new AddSpecResponse(null);
  }

  private  AddSpecResponse onAddFlowSpec(FlowSpec flowSpec) {
    Properties flowSpecProperties = flowSpec.getConfigAsProperties();
    if (topologySpecMap.containsKey(flowSpec.getUri())) {
      log.error("flow spec URI: {} is the same as one of the spec executors uris, ignore the flow", flowSpec.getUri());
      flowSpec.getCompilationErrors().add(new FlowSpec.CompilationError(0, "invalid flow spec uri " + flowSpec.getUri() + " because it is the same as one of the spec executors uri"));
      return null;
    }
    if (flowSpecProperties.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY) && StringUtils.isNotBlank(
        flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY))) {
      try {
        new CronExpression(flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY));
      } catch (Exception e) {
        log.error("invalid cron schedule: {}", flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY), e);
        flowSpec.getCompilationErrors().add(new FlowSpec.CompilationError(0, "invalid cron schedule: " + flowSpecProperties.getProperty(ConfigurationKeys.JOB_SCHEDULE_KEY) + e.getMessage()));
        return null;
      }
    }
    String response = null;

    // always try to compile the flow to verify if it is compilable
    Dag<JobExecutionPlan> dag = this.compileFlow(flowSpec);

    // If dag is null then a compilation error has occurred
    if (dag != null && !dag.isEmpty()) {
      response = dag.toString();
    }

    if (FlowCatalog.isCompileSuccessful(response) && this.userQuotaManager.isPresent() && !flowSpec.isExplain() &&
        (!flowSpec.getConfigAsProperties().containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY) || PropertiesUtils.getPropAsBoolean(flowSpec.getConfigAsProperties(), ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "false"))) {
      try {
        userQuotaManager.get().checkQuota(dag.getStartNodes());
        flowSpec.getConfigAsProperties().setProperty(ServiceConfigKeys.GOBBLIN_SERVICE_ADHOC_FLOW, "true");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return new AddSpecResponse<>(response);
  }

  @Override
  public AddSpecResponse onAddSpec(Spec addedSpec) {
    if (addedSpec instanceof FlowSpec) {
      return onAddFlowSpec((FlowSpec) addedSpec);
    } else if (addedSpec instanceof TopologySpec) {
      return onAddTopologySpec( (TopologySpec) addedSpec);
    }
    return new AddSpecResponse(null);
  }

  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    onDeleteSpec(deletedSpecURI, deletedSpecVersion, new Properties());
  }

  @Override
  public synchronized void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion, Properties headers) {
    if (topologySpecMap.containsKey(deletedSpecURI)) {
      topologySpecMap.remove(deletedSpecURI);
    }
  }

  @Override
  public synchronized void onUpdateSpec(Spec updatedSpec) {
    topologySpecMap.put(updatedSpec.getUri(), (TopologySpec) updatedSpec);
  }

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return null != this.metricContext;
  }

  @Override
  public List<Tag<?>> generateTags(State state){
      return Collections.emptyList();
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<URI, TopologySpec> getTopologySpecMap() {
    return this.topologySpecMap;
  }

  public abstract Dag<JobExecutionPlan> compileFlow(Spec spec);

  /**
   * Naive implementation of generating jobSpec, which fetch the first available template,
   * in an exemplified single-hop FlowCompiler implementation.
   * @param flowSpec
   * @return
   */
  protected JobSpec jobSpecGenerator(FlowSpec flowSpec) {
    JobSpec jobSpec;
    JobSpec.Builder jobSpecBuilder = JobSpec.builder(jobSpecURIGenerator(flowSpec))
        .withConfig(flowSpec.getConfig())
        .withDescription(flowSpec.getDescription())
        .withVersion(flowSpec.getVersion());

    if (flowSpec.getTemplateURIs().isPresent() && templateCatalog.isPresent()) {
      // Only first template uri will be honored for Identity
      jobSpecBuilder = jobSpecBuilder.withTemplate(flowSpec.getTemplateURIs().get().iterator().next());
      try {
        jobSpec = new ResolvedJobSpec(jobSpecBuilder.build(), templateCatalog.get());
        log.info("Resolved JobSpec properties are: " + PropertiesUtils.prettyPrintProperties(jobSpec.getConfigAsProperties()));
      } catch (SpecNotFoundException | JobTemplate.TemplateException e) {
        throw new RuntimeException("Could not resolve template in JobSpec from TemplateCatalog", e);
      }
    } else {
      jobSpec = jobSpecBuilder.build();
      log.info("Unresolved JobSpec properties are: " + jobSpec.getConfigAsProperties());
    }

    // Remove schedule
    jobSpec.setConfig(jobSpec.getConfig().withoutPath(ConfigurationKeys.JOB_SCHEDULE_KEY));

    // Add job.name and job.group
    if (flowSpec.getConfig().hasPath(ConfigurationKeys.FLOW_NAME_KEY)) {
      jobSpec.setConfig(jobSpec.getConfig()
          .withValue(ConfigurationKeys.JOB_NAME_KEY, flowSpec.getConfig().getValue(ConfigurationKeys.FLOW_NAME_KEY)));
    }
    if (flowSpec.getConfig().hasPath(ConfigurationKeys.FLOW_GROUP_KEY)) {
      jobSpec.setConfig(jobSpec.getConfig()
          .withValue(ConfigurationKeys.JOB_GROUP_KEY, flowSpec.getConfig().getValue(ConfigurationKeys.FLOW_GROUP_KEY)));
    }

    // Add flow execution id for this compilation
    long flowExecutionId = FlowUtils.getOrCreateFlowExecutionId(flowSpec);
    jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY,
        ConfigValueFactory.fromAnyRef(flowExecutionId)));

    // Reset properties in Spec from Config
    jobSpec.setConfigAsProperties(ConfigUtils.configToProperties(jobSpec.getConfig()));
    return jobSpec;
  }

  /**
   * It can receive multiple number of parameters, needed to generate a unique URI.
   * Implementation is flowSpecCompiler dependent.
   * This method should return URI which has job name at third place, when split by "/"
   * e.g. /flowGroup/flowName
   *      /flowGroup/flowName/sourceNode-targetNode
   * SafeDatasetCommit creates state store using this name and
   * {@link org.apache.gobblin.runtime.job_monitor.KafkaJobMonitor} extract job name to find the state store path.
   * @param objects
   * @return
   */
  public  URI jobSpecURIGenerator(Object... objects) {
    return ((FlowSpec)objects[0]).getUri();
  }

  /**
   * It returns the template uri for job.
   * This method can be overridden by derived FlowToJobSpecCompiler classes.
   * @param flowSpec
   * @return template URI
   */
  protected URI jobSpecTemplateURIGenerator(FlowSpec flowSpec) {
    // For now only first template uri will be honored for Identity
    return flowSpec.getTemplateURIs().get().iterator().next();
  }
}