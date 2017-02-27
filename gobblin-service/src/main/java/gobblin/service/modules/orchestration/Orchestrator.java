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

package gobblin.service.modules.orchestration;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.runtime.api.FlowSpec;
import gobblin.runtime.api.SpecCompiler;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.runtime.api.TopologySpec;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecCatalogListener;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.spec_catalog.FlowCatalog;
import gobblin.runtime.spec_catalog.TopologyCatalog;
import gobblin.service.ServiceConfigKeys;
import gobblin.util.ClassAliasResolver;
import org.slf4j.LoggerFactory;


/**
 * Orchestrator that is a {@link SpecCatalogListener}. It listens to changes
 * to {@link gobblin.runtime.spec_catalog.FlowCatalog} and {@link TopologyCatalog}
 * and:
 * If topology has changed: invokes FlowCompiler on all flows and provisions them on their respective executors
 * If flow has changed: invokes FlowCompiler on all that flow and provisions it on its respective executors
 */
public class Orchestrator implements SpecCatalogListener {
  protected final Logger _log;
  protected final SpecCompiler specCompiler;
  protected final Optional<FlowCatalog> flowCatalog;
  protected final Optional<TopologyCatalog> topologyCatalog;

  private final ClassAliasResolver<SpecCompiler> aliasResolver;

  public Orchestrator(Config config, Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog,
      Optional<Logger> log) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());

    this.aliasResolver = new ClassAliasResolver<>(SpecCompiler.class);
    this.flowCatalog = flowCatalog;
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

  public Orchestrator(Config config, Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog,
      Logger log) {
    this(config, flowCatalog, topologyCatalog, Optional.of(log));
  }

  public Orchestrator(Config config, Logger log) {
    this(config, Optional.<FlowCatalog>absent(), Optional.<TopologyCatalog>absent(), Optional.of(log));
  }

  /** Constructor with no logging */
  public Orchestrator(Config config, Optional<FlowCatalog> flowCatalog, Optional<TopologyCatalog> topologyCatalog) {
    this(config, flowCatalog, topologyCatalog, Optional.<Logger>absent());
  }

  public Orchestrator(Config config) {
    this(config, Optional.<FlowCatalog>absent(), Optional.<TopologyCatalog>absent(), Optional.<Logger>absent());
  }

  @VisibleForTesting
  public SpecCompiler getSpecCompiler() {
    return this.specCompiler;
  }

  /******************************************************************************
   *
   * At the moment below implementation handles only the following scenario:
   * - Topology is Immutable, it is read-only at bootstrap and does not changes
   *   between different executions
   * - Flow to Job mapping is 1:1 (including URI)
   * - Flow update is not supported. Addition / Deletion is handled
   *
   * Various cases to handle to make this multi-tenant and ready for dynamic topology:
   * - When Topology is added, all Flows should be re-compiled and re-provisioned
   *   (including deletion of the ones that are being relocated from previous SEIs)
   * - When Topology is deleted, all Flows should be re-compiled and re-provisioned
   * - When Topology is updated, all Flows should be re-compiled and re-provisioned
   * - When Flow is added, its compiled Jobs should be provisioned on active SEIs
   * - When Flow is updated, its previous compiled Jobs should be deleted from various
   *   SEIs and new Jobs provisioned on active SEIs
   * - When Flow is deleted, all its Jobs should be removed from SEIs
   * - These Mappings should be persisted as well as discoverable from SEIs
   *   (Right now since Flow : Job is 1 : 1 mapping, persisting Flow alone works well)
   *
   /*****************************************************************************/

  /** {@inheritDoc} */
  @Override
  public void onAddSpec(Spec addedSpec) {
    _log.info("New Spec detected: " + addedSpec);

    if (addedSpec instanceof FlowSpec) {
      Map<Spec, SpecExecutorInstanceProducer> specExecutorInstanceMap = specCompiler.compileFlow(addedSpec);

      if (specExecutorInstanceMap.isEmpty()) {
        _log.warn("Spec: " + addedSpec + " added, but cannot determine an executor to run on.");
        return;
      }

      // Using the first mapping, we will evolve to be more fancy in selecting which executor to run on later
      SpecExecutorInstanceProducer selectedExecutor = specExecutorInstanceMap.values().iterator().next();

      // Run this spec on this executor
      try {
        selectedExecutor.addSpec(addedSpec).get();
      } catch (InterruptedException | ExecutionException e) {
        _log.error("Cannot successfully setup spec: " + addedSpec + " on executor: " + selectedExecutor);
      }
    } else if (addedSpec instanceof TopologySpec) {
      this.specCompiler.onAddSpec(addedSpec);
      // TODO: Re-compile Flows. Since Topologies are immutable at the moment, we are not handling it.
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    _log.info("Spec deletion detected: " + deletedSpecURI + "/" + deletedSpecVersion);

    // To determine if the call was made via TopologyCatalog or FlowCatalog, iterate over StackTrace
    // Todo: Add an mechanism for easy discoverability of caller since URI can be of ToplogySpec or FlowSpec

    try {
      if (topologyCatalog.isPresent()) {
        topologyCatalog.get().getSpec(deletedSpecURI);
        this.specCompiler.onDeleteSpec(deletedSpecURI, deletedSpecVersion);
        // TODO: Re-compile Flows. Since Topologies are immutable at the moment, we are not handling it.
      }
    } catch (SpecNotFoundException e) {
      _log.info(String.format("Spec with URI: %s was not found in TopologyCatalog, going to try FlowCatalog",
          deletedSpecURI));
    }

    try {
      if (flowCatalog.isPresent()) {
        flowCatalog.get().getSpec(deletedSpecURI);
        // If deleted Spec is of type Flow, remove it from all SpecExecutorInstances
        // (SpecExecutorInstances are expected to no-op if they do not have the URI scheduled on them)
        if (topologyCatalog.isPresent()) {
          Collection<TopologySpec> specs = specCompiler.getTopologySpecMap().values();
          for (TopologySpec topologySpec : specs) {
            try {
              // Note: At the moment, the URI of Flow and Job is expected to remain the same.
              // .. We will need to handle 1 Flow URI to n Job URI compilation when we introduce it.
              // .. Hence at the moment Flow URI is being deleted on all SpecExecutorInstances.
              topologySpec.getSpecExecutorInstanceProducer().deleteSpec(deletedSpecURI);
            } catch (RuntimeException e) {
              _log.warn(String.format("Did not delete Spec with URI: %s and Version: %s on SpecExecutorInstance: %s",
                  deletedSpecURI, deletedSpecVersion, topologySpec.getSpecExecutorInstanceProducer().getUri()), e);
            }
          }
        }
      }
    } catch (SpecNotFoundException e) {
      _log.info(String.format("Spec with URI: %s was not found in TopologyCatalog, going to try FlowCatalog",
          deletedSpecURI));
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onUpdateSpec(Spec updatedSpec) {
    _log.info("Spec changed: " + updatedSpec);
    throw new UnsupportedOperationException();
  }
}
