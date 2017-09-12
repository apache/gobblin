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

package org.apache.gobblin.runtime.api;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import javax.annotation.concurrent.NotThreadSafe;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;

import lombok.AllArgsConstructor;
import lombok.Data;


/**
 * Data model representation that describes a topology ie. a {@link SpecExecutor} and its
 * capabilities tuple .
 *
 */
@Alpha
@Data
@AllArgsConstructor
@NotThreadSafe
public class TopologySpec implements Configurable, Spec {
  public static final String DEFAULT_SPEC_EXECUTOR_INSTANCE = InMemorySpecExecutor.class.getCanonicalName();
  public static final String SPEC_EXECUTOR_INSTANCE_KEY = "specExecutorInstance.class";

  private static final long serialVersionUID = 6106269076155338046L;

  /** An URI identifying the topology. */
  final URI uri;

  /** The implementation-defined version of this spec. */
  final String version;

  /** Human-readable description of the topology spec */
  final String description;

  /** Topology config as a typesafe config object*/
  @SuppressWarnings(justification="No bug", value="SE_BAD_FIELD")
  final Config config;

  /** Topology config as a properties collection for backwards compatibility */
  // Note that this property is not strictly necessary as it can be generated from the typesafe
  // config. We use it as a cache until typesafe config is more widely adopted in Gobblin.
  final Properties configAsProperties;

  /** Underlying executor instance such as Gobblin cluster or Azkaban */
  @SuppressWarnings(justification="Initialization handled by getter", value="SE_TRANSIENT_FIELD_NOT_RESTORED")
  transient SpecExecutor specExecutorInstance;

  /**
   * @return A {@link SpecExecutor}'s instance defined by <Technology, Location, Communication Mechanism>
   */
  public synchronized SpecExecutor getSpecExecutor() {
    if (null == specExecutorInstance) {
      String specExecutorClass = DEFAULT_SPEC_EXECUTOR_INSTANCE;
      if (config.hasPath(SPEC_EXECUTOR_INSTANCE_KEY)) {
        specExecutorClass = config.getString(SPEC_EXECUTOR_INSTANCE_KEY);
      }
      try {
        ClassAliasResolver<SpecExecutor> _aliasResolver =
            new ClassAliasResolver<>(SpecExecutor.class);
        specExecutorInstance = (SpecExecutor) ConstructorUtils
            .invokeConstructor(Class.forName(_aliasResolver
                .resolve(specExecutorClass)), config);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
          | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return specExecutorInstance;
  }

  public static TopologySpec.Builder builder(URI topologySpecUri) {
    return new TopologySpec.Builder(topologySpecUri);
  }

  public static TopologySpec.Builder builder(String topologySpecUri) {
    return new TopologySpec.Builder(topologySpecUri);
  }

  public static TopologySpec.Builder builder() {
    return new TopologySpec.Builder();
  }

  /** Creates a builder for the TopologySpec based on values in a topology properties config. */
  public static TopologySpec.Builder builder(URI catalogURI, Properties topologyProps) {
    String name = topologyProps.getProperty(ConfigurationKeys.TOPOLOGY_NAME_KEY);
    String group = topologyProps.getProperty(ConfigurationKeys.TOPOLOGY_GROUP_KEY, "default");

    try {
      URI topologyURI = new URI(catalogURI.getScheme(), catalogURI.getAuthority(),
          "/" + group + "/" + name, null);
      TopologySpec.Builder builder = new TopologySpec.Builder(topologyURI).withConfigAsProperties(topologyProps);
      String descr = topologyProps.getProperty(ConfigurationKeys.TOPOLOGY_DESCRIPTION_KEY, null);
      if (null != descr) {
        builder = builder.withDescription(descr);
      }

      return builder;
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to create a TopologySpec URI: " + e, e);
    }
  }

  public String toShortString() {
    return getUri().toString() + "/" + getVersion();
  }

  public String toLongString() {
    return getUri().toString() + "/" + getVersion() + "[" + getDescription() + "]";
  }

  @Override
  public String toString() {
    return toShortString();
  }

  /**
   * Builder for {@link TopologySpec}s.
   * <p> Defaults/conventions:
   * <ul>
   *  <li> Default topologyCatalogURI is {@link #DEFAULT_TOPOLOGY_CATALOG_SCHEME}:
   *  <li> Convention for TopologySpec URI: <topologyCatalogURI>/config.get({@link ConfigurationKeys#TOPOLOGY_GROUP_KEY})/config.get({@link ConfigurationKeys#TOPOLOGY_NAME_KEY})
   *  <li> Convention for Description: config.get({@link ConfigurationKeys#TOPOLOGY_DESCRIPTION_KEY})
   *  <li> Default version: 1
   * </ul>
   */
  public static class Builder {
    public static final String DEFAULT_TOPOLOGY_CATALOG_SCHEME = "gobblin-topology";
    @VisibleForTesting
    private Optional<Config> config = Optional.absent();
    private Optional<Properties> configAsProperties = Optional.absent();
    private Optional<URI> uri;
    private String version = "1";
    private Optional<String> description = Optional.absent();
    private Optional<URI> topologyCatalogURI = Optional.absent();
    private Optional<SpecExecutor> specExecutorInstance = Optional.absent();

    public Builder(URI topologySpecUri) {
      Preconditions.checkNotNull(topologySpecUri);
      this.uri = Optional.of(topologySpecUri);
    }

    public Builder(String topologySpecUri) {
      Preconditions.checkNotNull(topologySpecUri);
      Preconditions.checkNotNull(topologySpecUri);
      try {
        this.uri = Optional.of(new URI(topologySpecUri));
      }
      catch (URISyntaxException e) {
        throw new RuntimeException("Invalid TopologySpec config: " + e, e);
      }
    }

    public Builder() {
      this.uri = Optional.absent();
    }

    public TopologySpec build() {
      Preconditions.checkNotNull(this.uri);
      Preconditions.checkNotNull(this.version);
      return new TopologySpec(getURI(), getVersion(), getDescription(), getConfig(), getConfigAsProperties(),
          getSpecExceutorInstance());

    }

    /** The scheme and authority of the topology catalog URI are used to generate TopologySpec URIs from
     * topology configs. */
    public TopologySpec.Builder withTopologyCatalogURI(URI topologyCatalogURI) {
      this.topologyCatalogURI = Optional.of(topologyCatalogURI);
      return this;
    }

    public TopologySpec.Builder withTopologyCatalogURI(String topologyCatalogURI) {
      try {
        this.topologyCatalogURI = Optional.of(new URI(topologyCatalogURI));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Unable to set topology catalog URI: " + e, e);
      }
      return this;
    }

    public URI getDefaultTopologyCatalogURI() {
      try {
        return new URI(DEFAULT_TOPOLOGY_CATALOG_SCHEME, null, "/", null, null);
      } catch (URISyntaxException e) {
        // should not happen
        throw new Error("Unexpected exception: " + e, e);
      }
    }

    public URI getTopologyCatalogURI() {
      if (! this.topologyCatalogURI.isPresent()) {
        this.topologyCatalogURI = Optional.of(getDefaultTopologyCatalogURI());
      }
      return this.topologyCatalogURI.get();
    }

    public URI getDefaultURI() {
      URI topologyCatalogURI = getTopologyCatalogURI();
      Config topologyCfg = getConfig();
      String name = topologyCfg.hasPath(ConfigurationKeys.TOPOLOGY_NAME_KEY) ?
          topologyCfg.getString(ConfigurationKeys.TOPOLOGY_NAME_KEY) :
          "default";
      String group = topologyCfg.hasPath(ConfigurationKeys.TOPOLOGY_GROUP_KEY) ?
          topologyCfg.getString(ConfigurationKeys.TOPOLOGY_GROUP_KEY) :
          "default";
      try {
        return new URI(topologyCatalogURI.getScheme(), topologyCatalogURI.getAuthority(),
            "/" + group + "/" + name, null, null);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Unable to create default TopologySpec URI:" + e, e);
      }
    }

    public URI getURI() {
      if (! this.uri.isPresent()) {
        this.uri = Optional.of(getDefaultURI());
      }

      return this.uri.get();
    }

    public TopologySpec.Builder withVersion(String version) {
      Preconditions.checkNotNull(version);
      this.version = version;
      return this;
    }

    public String getVersion() {
      return this.version;
    }

    public TopologySpec.Builder withDescription(String topologyDescription) {
      Preconditions.checkNotNull(topologyDescription);
      this.description = Optional.of(topologyDescription);
      return this;
    }

    public String getDefaultDescription() {
      Config topologyConf = getConfig();
      return topologyConf.hasPath(ConfigurationKeys.TOPOLOGY_DESCRIPTION_KEY) ?
          topologyConf.getString(ConfigurationKeys.TOPOLOGY_DESCRIPTION_KEY) :
          "Gobblin topology " + getURI();
    }

    public String getDescription() {
      if (! this.description.isPresent()) {
        this.description = Optional.of(getDefaultDescription());
      }
      return this.description.get();
    }

    public Config getDefaultConfig() {
      return ConfigFactory.empty();
    }

    public Config getConfig() {
      if (!this.config.isPresent()) {
        this.config = this.configAsProperties.isPresent() ?
            Optional.of(ConfigUtils.propertiesToTypedConfig(this.configAsProperties.get(),
                Optional.<String>absent())) :
            Optional.of(getDefaultConfig());
      }
      return this.config.get();
    }

    public TopologySpec.Builder withConfig(Config topologyConfig) {
      Preconditions.checkNotNull(topologyConfig);
      this.config = Optional.of(topologyConfig);
      return this;
    }

    public Properties getConfigAsProperties() {
      if (!this.configAsProperties.isPresent()) {
        this.configAsProperties = Optional.of(ConfigUtils.configToProperties(this.config.get()));
      }
      return this.configAsProperties.get();
    }

    public TopologySpec.Builder withConfigAsProperties(Properties topologyConfig) {
      Preconditions.checkNotNull(topologyConfig);
      this.configAsProperties = Optional.of(topologyConfig);
      return this;
    }

    public SpecExecutor getSpecExceutorInstance() {
      if (!this.specExecutorInstance.isPresent()) {
        // TODO: Try to init SpecProducer from config if not initialized via builder.
        throw new RuntimeException("SpecExecutor not initialized.");
      }
      return this.specExecutorInstance.get();
    }

    public TopologySpec.Builder withSpecExecutor(SpecExecutor specExecutor) {
      Preconditions.checkNotNull(specExecutor);
      this.specExecutorInstance = Optional.of(specExecutor);
      return this;
    }
  }

  /**
   * get the private uri as the primary key for this object.
   * @return
   */
  public URI getUri() {
    return this.uri;
  }

}