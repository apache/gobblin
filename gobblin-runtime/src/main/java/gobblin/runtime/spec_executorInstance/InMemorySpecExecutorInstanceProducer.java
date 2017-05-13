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

package gobblin.runtime.spec_executorInstance;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.util.CompletedFuture;


public class InMemorySpecExecutorInstanceProducer implements SpecExecutorInstanceProducer<Spec>, Serializable {

  private static final Splitter SPLIT_BY_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  private static final Splitter SPLIT_BY_COLON = Splitter.on(":").omitEmptyStrings().trimResults();

  private static final long serialVersionUID = 6106269076155338045L;

  protected final transient Logger log;
  protected final Map<URI, Spec> provisionedSpecs;
  @SuppressWarnings (justification="No bug", value="SE_BAD_FIELD")
  protected final Config config;
  protected final Map<String, String> capabilities;

  public InMemorySpecExecutorInstanceProducer(Config config) {
    this(config, Optional.<Logger>absent());
  }

  public InMemorySpecExecutorInstanceProducer(Config config, GobblinInstanceEnvironment env) {
    this(config, Optional.of(env.getLog()));
  }

  public InMemorySpecExecutorInstanceProducer(Config config, Optional<Logger> log) {
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.config = config;
    this.provisionedSpecs = Maps.newHashMap();
    this.capabilities = Maps.newHashMap();
    if (config.hasPath(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY)) {
      String capabilitiesStr = config.getString(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY);
      List<String> capabilities = SPLIT_BY_COMMA.splitToList(capabilitiesStr);
      for (String capability : capabilities) {
        List<String> currentCapability = SPLIT_BY_COLON.splitToList(capability);
        Preconditions.checkArgument(currentCapability.size() == 2, "Only one source:destination pair is supported "
            + "per capability, found: " + currentCapability);
        this.capabilities.put(currentCapability.get(0), currentCapability.get(1));
      }
    }
  }

  @Override
  public URI getUri() {
    try {
      return new URI("InMemorySpecExecutorInstanceProducer");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Future<String> getDescription() {
    return new CompletedFuture("InMemory SpecExecutorInstanceProducer", null);
  }

  @Override
  public Future<Config> getConfig() {
    return new CompletedFuture(this.config, null);
  }

  @Override
  public Future<String> getHealth() {
    return new CompletedFuture("Healthy", null);
  }

  @Override
  public Future<? extends Map<String, String>> getCapabilities() {
    return new CompletedFuture(this.capabilities, null);
  }

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    provisionedSpecs.put(addedSpec.getUri(), addedSpec);
    log.info(String.format("Added Spec: %s with Uri: %s for execution on this executor.", addedSpec, addedSpec.getUri()));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    if (!provisionedSpecs.containsKey(updatedSpec.getUri())) {
      throw new RuntimeException("Spec not found: " + updatedSpec.getUri());
    }
    provisionedSpecs.put(updatedSpec.getUri(), updatedSpec);
    log.info(String.format("Updated Spec: %s with Uri: %s for execution on this executor.", updatedSpec, updatedSpec.getUri()));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI) {
    if (!provisionedSpecs.containsKey(deletedSpecURI)) {
      throw new RuntimeException("Spec not found: " + deletedSpecURI);
    }
    provisionedSpecs.remove(deletedSpecURI);
    log.info(String.format("Deleted Spec with Uri: %s from this executor.", deletedSpecURI));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<? extends List<Spec>> listSpecs() {
    return new CompletedFuture<>(Lists.newArrayList(provisionedSpecs.values()), null);
  }
}
