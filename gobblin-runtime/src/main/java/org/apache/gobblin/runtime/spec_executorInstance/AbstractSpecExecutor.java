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

package org.apache.gobblin.runtime.spec_executorInstance;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import org.apache.gobblin.runtime.api.SpecConsumer;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.util.CompletedFuture;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;


/**
 * An abstract implementation of SpecExecutor without specifying communication mechanism.
 *
 * Normally in the implementation of {@link AbstractSpecExecutor}, it is necessary to specify:
 * {@link SpecProducer}
 * {@link SpecConsumer}
 * {@link Closer}
 */
public abstract class AbstractSpecExecutor extends AbstractIdleService implements SpecExecutor {

  private static final Splitter SPLIT_BY_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  private static final Splitter SPLIT_BY_COLON = Splitter.on(":").omitEmptyStrings().trimResults();

  protected final transient Logger log;

  // Executor Instance identifier
  protected final URI specExecutorInstanceUri;

  @SuppressWarnings(justification = "No bug", value = "SE_BAD_FIELD")
  protected final Config config;

  protected final Map<ServiceNode, ServiceNode> capabilities;

  /**
   * While AbstractSpecExecutor is up, for most producer implementations (like SimpleKafkaSpecProducer),
   * they implements {@link java.io.Closeable} which requires registration and close methods.
   * {@link Closer} is mainly used for managing {@link SpecProducer} and {@link SpecConsumer}.
   */
  protected Optional<Closer> optionalCloser;

  public AbstractSpecExecutor(Config config) {
    this(config, Optional.<Logger>absent());
  }

  public AbstractSpecExecutor(Config config, GobblinInstanceEnvironment env) {
    this(config, Optional.of(env.getLog()));
  }

  public AbstractSpecExecutor(Config config, Optional<Logger> log) {

    /**
     * Since URI is regarded as the unique identifier for {@link SpecExecutor}(Used in equals method)
     * it is dangerous to use default URI.
     */
    if (!config.hasPath(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY)) {
      if (log.isPresent()) {
        log.get().warn("The SpecExecutor doesn't specify URI, using the default one.");
      }
    }

    try {
      specExecutorInstanceUri =
          new URI(ConfigUtils.getString(config, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, "NA"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    this.log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.config = config;
    this.capabilities = Maps.newHashMap();
    if (config.hasPath(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY)) {
      String capabilitiesStr = config.getString(ConfigurationKeys.SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY);
      List<String> capabilities = SPLIT_BY_COMMA.splitToList(capabilitiesStr);
      for (String capability : capabilities) {
        List<String> currentCapability = SPLIT_BY_COLON.splitToList(capability);
        Preconditions.checkArgument(currentCapability.size() == 2,
            "Only one source:destination pair is supported " + "per capability, found: " + currentCapability);
        this.capabilities.put(new BaseServiceNodeImpl(currentCapability.get(0)),
            new BaseServiceNodeImpl(currentCapability.get(1)));
      }
    }
    optionalCloser = Optional.absent();
  }

  @Override
  public URI getUri() {
    return specExecutorInstanceUri;
  }

  /**
   * The definition of attributes are the technology that a {@link SpecExecutor} is using and
   * the physical location that it runs on.
   *
   * These attributes are supposed to be static and read-only.
   */
  @Override
  public Config getAttrs() {
    Preconditions.checkArgument(this.config.hasPath(ServiceConfigKeys.ATTRS_PATH_IN_CONFIG),
        "Input configuration doesn't contains SpecExecutor Attributes path.");
    return this.config.getConfig(ServiceConfigKeys.ATTRS_PATH_IN_CONFIG);
  }

  @Override
  public Future<Config> getConfig() {
    return new CompletedFuture(this.config, null);
  }

  @Override
  public Future<? extends Map<ServiceNode, ServiceNode>> getCapabilities() {
    return new CompletedFuture(this.capabilities, null);
  }

  /**
   * Two {@link SpecExecutor}s with the same {@link #specExecutorInstanceUri}
   * should be considered as the same {@link SpecExecutor}.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractSpecExecutor that = (AbstractSpecExecutor) o;

    return specExecutorInstanceUri.equals(that.specExecutorInstanceUri);
  }

  @Override
  public int hashCode() {
    return specExecutorInstanceUri.hashCode();
  }

  /**
   * @return In default implementation we just return 'Healthy'.
   */
  @Override
  public Future<String> getHealth() {
    return new CompletedFuture("Healthy", null);
  }

  abstract protected void startUp() throws Exception;

  abstract protected void shutDown() throws Exception;

  abstract public Future<? extends SpecProducer> getProducer();

  abstract public Future<String> getDescription();
}
