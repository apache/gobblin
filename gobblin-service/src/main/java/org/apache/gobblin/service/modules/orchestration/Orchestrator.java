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

package org.apache.gobblin.service.modules.orchestration;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecCompiler;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.ServiceMetricNames;
import org.apache.gobblin.service.modules.flow.IdentityFlowToJobSpecCompiler;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.slf4j.LoggerFactory;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;

import lombok.Getter;


/**
 * Orchestrator that is a {@link SpecCatalogListener}. It listens to changes
 * to {@link TopologyCatalog} and updates {@link SpecCompiler} state.
 */
@Alpha
public class Orchestrator implements SpecCatalogListener, Instrumentable {
  protected final Logger _log;
  protected final SpecCompiler specCompiler;
  protected final Optional<TopologyCatalog> topologyCatalog;

  protected final MetricContext metricContext;
  @Getter
  private Optional<Meter> flowOrchestrationSuccessFulMeter;
  @Getter
  private Optional<Meter> flowOrchestrationFailedMeter;
  @Getter
  private Optional<Timer> flowOrchestrationTimer;

  private final ClassAliasResolver<SpecCompiler> aliasResolver;

  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog, Optional<Logger> log, boolean instrumentationEnabled) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    if (instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), IdentityFlowToJobSpecCompiler.class);
      this.flowOrchestrationSuccessFulMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_SUCCESSFUL_METER));
      this.flowOrchestrationFailedMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_FAILED_METER));
      this.flowOrchestrationTimer = Optional.<Timer>of(this.metricContext.timer(ServiceMetricNames.FLOW_ORCHESTRATION_TIMER));
    }
    else {
      this.metricContext = null;
      this.flowOrchestrationSuccessFulMeter = Optional.absent();
      this.flowOrchestrationFailedMeter = Optional.absent();
      this.flowOrchestrationTimer = Optional.absent();
    }

    this.aliasResolver = new ClassAliasResolver<>(SpecCompiler.class);
    this.topologyCatalog = topologyCatalog;
    try {
      String specCompilerClassName = ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS;
      if (config.hasPath(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY)) {
        specCompilerClassName = config.getString(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY);
      }
      _log.info("Using specCompiler class name/alias " + specCompilerClassName);

      this.specCompiler = (SpecCompiler) ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(
          specCompilerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog, Optional<Logger> log) {
    this(config, topologyCatalog, log, true);
  }

  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog, Logger log) {
    this(config, topologyCatalog, Optional.of(log));
  }

  public Orchestrator(Config config, Logger log) {
    this(config, Optional.<TopologyCatalog>absent(), Optional.of(log));
  }

  /** Constructor with no logging */
  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog) {
    this(config, topologyCatalog, Optional.<Logger>absent());
  }

  public Orchestrator(Config config) {
    this(config, Optional.<TopologyCatalog>absent(), Optional.<Logger>absent());
  }

  @VisibleForTesting
  public SpecCompiler getSpecCompiler() {
    return this.specCompiler;
  }

  /** {@inheritDoc} */
  @Override
  public void onAddSpec(Spec addedSpec) {
    if (addedSpec instanceof TopologySpec) {
      _log.info("New Spec detected of type TopologySpec: " + addedSpec);
      this.specCompiler.onAddSpec(addedSpec);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    _log.info("Spec deletion detected: " + deletedSpecURI + "/" + deletedSpecVersion);

    if (topologyCatalog.isPresent()) {
      this.specCompiler.onDeleteSpec(deletedSpecURI, deletedSpecVersion);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onUpdateSpec(Spec updatedSpec) {
    _log.info("Spec changed: " + updatedSpec);

    if (!(updatedSpec instanceof TopologySpec)) {
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

  public void orchestrate(Spec spec) throws Exception {
    // Add below waiting because TopologyCatalog and FlowCatalog service can be launched at the same time
    this.topologyCatalog.get().getInitComplete().await();

    long startTime = System.nanoTime();
    if (spec instanceof FlowSpec) {
      Map<Spec, SpecExecutor> specExecutorInstanceMap = specCompiler.compileFlow(spec);

      if (specExecutorInstanceMap.isEmpty()) {
        _log.warn("Cannot determine an executor to run on for Spec: " + spec);
        return;
      }

      // Schedule all compiled JobSpecs on their respective Executor
      for (Map.Entry<Spec, SpecExecutor> specsToExecute : specExecutorInstanceMap.entrySet()) {
        // Run this spec on selected executor
        SpecProducer producer = null;
        try {
          producer = specsToExecute.getValue().getProducer().get();
          Spec jobSpec = specsToExecute.getKey();

          _log.info(String.format("Going to orchestrate JobSpec: %s on Executor: %s", jobSpec, producer));
          producer.addSpec(jobSpec);
        } catch(Exception e) {
          _log.error("Cannot successfully setup spec: " + specsToExecute.getKey() + " on executor: " + producer +
              " for flow: " + spec, e);
        }
      }
    } else {
      Instrumented.markMeter(this.flowOrchestrationFailedMeter);
      throw new RuntimeException("Spec not of type FlowSpec, cannot orchestrate: " + spec);
    }
    Instrumented.markMeter(this.flowOrchestrationSuccessFulMeter);
    Instrumented.updateTimer(this.flowOrchestrationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
  }

  public void remove(Spec spec) {
    // TODO: Evolve logic to cache and reuse previously compiled JobSpecs
    // .. this will work for Identity compiler but not always for multi-hop.
    // Note: Current logic assumes compilation is consistent between all executions
    if (spec instanceof FlowSpec) {
      Map<Spec, SpecExecutor> specExecutorInstanceMap = specCompiler.compileFlow(spec);

      if (specExecutorInstanceMap.isEmpty()) {
        _log.warn("Cannot determine an executor to delete Spec: " + spec);
        return;
      }

      // Delete all compiled JobSpecs on their respective Executor
      for (Map.Entry<Spec, SpecExecutor> specsToDelete : specExecutorInstanceMap.entrySet()) {
        // Delete this spec on selected executor
        SpecProducer producer = null;
        try {
          producer = specsToDelete.getValue().getProducer().get();
          Spec jobSpec = specsToDelete.getKey();

          _log.info(String.format("Going to delete JobSpec: %s on Executor: %s", jobSpec, producer));
          producer.deleteSpec(jobSpec.getUri());
        } catch(Exception e) {
          _log.error("Cannot successfully delete spec: " + specsToDelete.getKey() + " on executor: " + producer +
              " for flow: " + spec, e);
        }
      }
    } else {
      throw new RuntimeException("Spec not of type FlowSpec, cannot delete: " + spec);
    }
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
  public List<Tag<?>> generateTags(State state) {
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