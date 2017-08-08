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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.SpecExecutorInstance;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;


public class AzkabanSpecExecutorInstance extends AbstractIdleService implements SpecExecutorInstance {
  protected static final Splitter SPLIT_BY_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  protected static final Splitter SPLIT_BY_COLON = Splitter.on(":").omitEmptyStrings().trimResults();

  // Executor Instance
  protected final Config _config;
  protected final Logger _log;
  protected final URI _specExecutorInstanceUri;
  protected final Map<String, String> _capabilities;

  public AzkabanSpecExecutorInstance(Config config, Optional<Logger> log) {
    _config = config;
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    try {
      _specExecutorInstanceUri = new URI(ConfigUtils.getString(config, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,
          "NA"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    _capabilities = Maps.newHashMap();
    if (config.hasPath(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY)) {
      String capabilitiesStr = config.getString(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY);
      List<String> capabilities = SPLIT_BY_COMMA.splitToList(capabilitiesStr);
      for (String capability : capabilities) {
        List<String> currentCapability = SPLIT_BY_COLON.splitToList(capability);
        Preconditions.checkArgument(currentCapability.size() == 2, "Only one source:destination pair is supported "
            + "per capability, found: " + currentCapability);
        _capabilities.put(currentCapability.get(0), currentCapability.get(1));
      }
    }
  }

  @Override
  public URI getUri() {
    return _specExecutorInstanceUri;
  }

  @Override
  public Future<String> getDescription() {
    return new CompletedFuture<>("SimpleSpecExecutorInstance with URI: " + _specExecutorInstanceUri, null);
  }

  @Override
  public Future<Config> getConfig() {
    return new CompletedFuture<>(_config, null);
  }

  @Override
  public Future<String> getHealth() {
    return new CompletedFuture<>("Healthy", null);
  }

  @Override
  public Future<? extends Map<String, String>> getCapabilities() {
    return new CompletedFuture<>(_capabilities, null);
  }

  @Override
  protected void startUp() throws Exception {
    // nothing to do in default implementation
  }

  @Override
  protected void shutDown() throws Exception {
    // nothing to do in default implementation
  }
}
