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

package org.apache.gobblin.service.modules.topology;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.runtime.api.SpecExecutor;



@Alpha
public class ConfigBasedTopologySpecFactory implements TopologySpecFactory {

  private static final Splitter SPLIT_BY_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  private final Config _config;
  private final Logger _log;
  private final ClassAliasResolver<SpecExecutor> _aliasResolver;

  public ConfigBasedTopologySpecFactory(Config config) {
    this(config, Optional.<Logger>absent());
  }

  public ConfigBasedTopologySpecFactory(Config config, Optional<Logger> log) {
    Preconditions.checkNotNull(config, "Config should not be null");
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _config = config;
    _aliasResolver = new ClassAliasResolver<>(SpecExecutor.class);
  }

  @Override
  public Collection<TopologySpec> getTopologies() {

    if (!_config.hasPath(ServiceConfigKeys.TOPOLOGY_FACTORY_TOPOLOGY_NAMES_KEY)) {
      return Collections.EMPTY_LIST;
    }
    Collection<TopologySpec> topologySpecs = Lists.newArrayList();
    Collection<String> topologyNames = SPLIT_BY_COMMA.splitToList(
        _config.getString(ServiceConfigKeys.TOPOLOGY_FACTORY_TOPOLOGY_NAMES_KEY));

    for (String topologyName : topologyNames) {
      Preconditions.checkArgument(_config.hasPath(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX + topologyName),
          "Config does not contain Topology Factory descriptor for Topology " + topologyName);
      Config topologyConfig = _config.getConfig(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX + topologyName);
      String description = ConfigUtils.getString(topologyConfig, ServiceConfigKeys.TOPOLOGYSPEC_DESCRIPTION_KEY, "NA");
      String version = ConfigUtils.getString(topologyConfig, ServiceConfigKeys.TOPOLOGYSPEC_VERSION_KEY, "-1");

      String specExecutorClass = ServiceConfigKeys.DEFAULT_SPEC_EXECUTOR;
      if (topologyConfig.hasPath(ServiceConfigKeys.SPEC_EXECUTOR_KEY)) {
        specExecutorClass = topologyConfig.getString(ServiceConfigKeys.SPEC_EXECUTOR_KEY);
      }
      SpecExecutor specExecutor;
      try {
        _log.info("Using SpecProducer class name/alias " + specExecutorClass);
        specExecutor = (SpecExecutor) ConstructorUtils
            .invokeConstructor(Class.forName(_aliasResolver
                .resolve(specExecutorClass)), topologyConfig);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
          | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      TopologySpec.Builder topologySpecBuilder = TopologySpec
          .builder(topologyConfig.getString(ServiceConfigKeys.TOPOLOGYSPEC_URI_KEY))
          .withConfig(topologyConfig)
          .withDescription(description)
          .withVersion(version)
          .withSpecExecutor(specExecutor);
      topologySpecs.add(topologySpecBuilder.build());
    }

    return topologySpecs;
  }
}