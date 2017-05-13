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

package gobblin.service.modules.flow;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import gobblin.annotation.Alpha;
import gobblin.configuration.ConfigurationKeys;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.api.FlowSpec;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecCompiler;
import gobblin.runtime.api.SpecExecutorInstance;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.api.TopologySpec;
import gobblin.runtime.job_catalog.FSJobCatalog;
import gobblin.runtime.job_spec.ResolvedJobSpec;
import gobblin.service.ServiceConfigKeys;
import gobblin.service.ServiceMetricNames;
import gobblin.util.ConfigUtils;


/***
 * Take in a logical {@link Spec} ie flow and compile corresponding materialized job {@link Spec}
 * and its mapping to {@link SpecExecutorInstance}.
 */
@Alpha
public class IdentityFlowToJobSpecCompiler implements SpecCompiler {

  private final Map<URI, TopologySpec> topologySpecMap;
  private final Config config;
  private final Logger log;
  private final Optional<FSJobCatalog> templateCatalog;

  protected final MetricContext metricContext;
  @Getter
  private Optional<Meter> flowCompilationSuccessFulMeter;
  @Getter
  private Optional<Meter> flowCompilationFailedMeter;
  @Getter
  private Optional<Timer> flowCompilationTimer;

  public IdentityFlowToJobSpecCompiler(Config config) {
    this(config, true);
  }

  public IdentityFlowToJobSpecCompiler(Config config, boolean instrumentationEnabled) {
    this(config, Optional.<Logger>absent(), instrumentationEnabled);
  }

  public IdentityFlowToJobSpecCompiler(Config config, Optional<Logger> log) {
    this(config, log, true);
  }

  public IdentityFlowToJobSpecCompiler(Config config, Optional<Logger> log, boolean instrumentationEnabled) {
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    if (instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), IdentityFlowToJobSpecCompiler.class);
      this.flowCompilationSuccessFulMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_COMPILATION_SUCCESSFUL_METER));
      this.flowCompilationFailedMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_COMPILATION_FAILED_METER));
      this.flowCompilationTimer = Optional.<Timer>of(this.metricContext.timer(ServiceMetricNames.FLOW_COMPILATION_TIMER));
    }
    else {
      this.metricContext = null;
      this.flowCompilationSuccessFulMeter = Optional.absent();
      this.flowCompilationFailedMeter = Optional.absent();
      this.flowCompilationTimer = Optional.absent();
    }

    this.topologySpecMap = Maps.newConcurrentMap();
    this.config = config;
    /***
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
      throw new RuntimeException("Could not initialize IdentityFlowToJobSpecCompiler because of "
          + "TemplateCatalog initialization failure", e);
    }
  }

  @Override
  public Map<Spec, SpecExecutorInstanceProducer> compileFlow(Spec spec) {
    Preconditions.checkNotNull(spec);
    Preconditions.checkArgument(spec instanceof FlowSpec, "IdentityFlowToJobSpecCompiler only converts FlowSpec to JobSpec");

    long startTime = System.nanoTime();
    Map<Spec, SpecExecutorInstanceProducer> specExecutorInstanceMap = Maps.newLinkedHashMap();

    FlowSpec flowSpec = (FlowSpec) spec;
    String source = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY);
    String destination = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY);
    log.info(String.format("Compiling flow for source: %s and destination: %s", source, destination));

    JobSpec jobSpec;
    JobSpec.Builder jobSpecBuilder = JobSpec.builder(flowSpec.getUri())
        .withConfig(flowSpec.getConfig())
        .withDescription(flowSpec.getDescription())
        .withVersion(flowSpec.getVersion());

    if (flowSpec.getTemplateURIs().isPresent() && templateCatalog.isPresent()) {
      // Only first template uri will be honored for Identity
      jobSpecBuilder = jobSpecBuilder.withTemplate(flowSpec.getTemplateURIs().get().iterator().next());
      try {
        jobSpec = new ResolvedJobSpec(jobSpecBuilder.build(), templateCatalog.get());
        log.info("Resolved JobSpec properties are: " + jobSpec.getConfigAsProperties());
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
    long flowExecutionId = System.currentTimeMillis();
    jobSpec.setConfig(jobSpec.getConfig().withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY,
        ConfigValueFactory.fromAnyRef(flowExecutionId)));

    // Reset properties in Spec from Config
    jobSpec.setConfigAsProperties(ConfigUtils.configToProperties(jobSpec.getConfig()));

    for (TopologySpec topologySpec : topologySpecMap.values()) {
      try {
        Map<String, String> capabilities = (Map<String, String>) topologySpec.getSpecExecutorInstanceProducer().getCapabilities().get();
        for (Map.Entry<String, String> capability : capabilities.entrySet()) {
          log.info(String.format("Evaluating current JobSpec: %s against TopologySpec: %s with "
              + "capability of source: %s and destination: %s ", jobSpec.getUri(),
              topologySpec.getUri(), capability.getKey(), capability.getValue()));
          if (source.equals(capability.getKey()) && destination.equals(capability.getValue())) {
            specExecutorInstanceMap.put(jobSpec, topologySpec.getSpecExecutorInstanceProducer());
            log.info(String.format("Current JobSpec: %s is executable on TopologySpec: %s. Added TopologySpec as candidate.",
                jobSpec.getUri(), topologySpec.getUri()));

            log.info("Since we found a candidate executor, we will not try to compute more. "
                + "(Intended limitation for IdentityFlowToJobSpecCompiler)");
            return specExecutorInstanceMap;
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        Instrumented.markMeter(this.flowCompilationFailedMeter);
        throw new RuntimeException("Cannot determine topology capabilities", e);
      }
    }
    Instrumented.markMeter(this.flowCompilationSuccessFulMeter);
    Instrumented.updateTimer(this.flowCompilationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

    return specExecutorInstanceMap;
  }

  @Override
  public Map<URI, TopologySpec> getTopologySpecMap() {
    return this.topologySpecMap;
  }

  @Override
  public void onAddSpec(Spec addedSpec) {
    topologySpecMap.put(addedSpec.getUri(), (TopologySpec) addedSpec);
  }

  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    topologySpecMap.remove(deletedSpecURI);
  }

  @Override
  public void onUpdateSpec(Spec updatedSpec) {
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
  public List<Tag<?>> generateTags(gobblin.configuration.State state) {
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
}
