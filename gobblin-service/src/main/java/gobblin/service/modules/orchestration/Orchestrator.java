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

import gobblin.runtime.spec_catalog.TopologyCatalog;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.runtime.api.FlowSpec;
import gobblin.runtime.api.SpecCompiler;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.runtime.api.TopologySpec;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecCatalogListener;
import gobblin.service.ServiceConfigKeys;
import gobblin.util.ClassAliasResolver;
import org.slf4j.LoggerFactory;


/**
 * Orchestrator that is a {@link SpecCatalogListener}. It listens to changes
 * to {@link gobblin.service.modules.flow.FlowCatalog} and {@link TopologyCatalog}
 * and:
 * If topology has changed: invokes FlowCompiler on all flows and provisions them on their respective executors
 * If flow has changed: invokes FlowCompiler on all that flow and provisions it on its respective executors
 */
public class Orchestrator implements SpecCatalogListener {
  protected final Logger _log;
  protected final SpecCompiler specCompiler;

  private final ClassAliasResolver<SpecCompiler> aliasResolver;

  public Orchestrator(Config config, Optional<Logger> log) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());

    this.aliasResolver = new ClassAliasResolver<>(SpecCompiler.class);
    try {
      String specCompilerClassName = ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS;
      if (config.hasPath(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY)) {
        specCompilerClassName = config.getString(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY);
      }
      _log.info("Using specCompiler class name/alias " + specCompilerClassName);

      this.specCompiler = (SpecCompiler) ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(
          specCompilerClassName)));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Orchestrator(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public Orchestrator(Config config) {
    this(config, Optional.<Logger>absent());
  }

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
        _log.error("Cannot successfully setup spec: " + addedSpec + " on excutor: " + selectedExecutor);
      }
    } else if (addedSpec instanceof TopologySpec) {
      // TODO: Re-provision all jobs since topology has changed.
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    _log.info("Spec deleted: " + deletedSpecURI + "/" + deletedSpecVersion);
  }

  /** {@inheritDoc} */
  @Override
  public void onUpdateSpec(Spec updatedSpec) {
    _log.info("Spec changed: " + updatedSpec);
  }
}
