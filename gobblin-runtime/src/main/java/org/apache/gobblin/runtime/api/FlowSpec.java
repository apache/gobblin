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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.typesafe.config.Config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

import lombok.Data;


/**
 * Defines a Gobblin Flow (potentially collection of {@link FlowSpec}) that can be run once, or multiple times.
 * A {@link FlowSpec} is {@link Configurable} so it has an associated {@link Config}, along with
 * other mandatory properties such as a uri, description, and version. A {@link FlowSpec} is
 * uniquely identified by its uri (containing group and name).
 *
 */
@Alpha
@Data
public class FlowSpec implements Configurable, Spec {
  /** An URI identifying the flow. */
  final URI uri;

  /** The implementation-defined version of this spec. */
  final String version;

  /** Human-readable description of the flow spec */
  final String description;

  /** Flow config as a typesafe config object*/
  final Config config;

  /** Flow config as a properties collection for backwards compatibility */
  // Note that this property is not strictly necessary as it can be generated from the typesafe
  // config. We use it as a cache until typesafe config is more widely adopted in Gobblin.
  final Properties configAsProperties;

  /** URI of {@link org.apache.gobblin.runtime.api.JobTemplate} to use. */
  final Optional<Set<URI>> templateURIs;

  /** Child {@link Spec}s to this {@link FlowSpec} **/
  // Note that a FlowSpec can be materialized into multiple FlowSpec or JobSpec hierarchy
  final Optional<List<Spec>> childSpecs;

  public static FlowSpec.Builder builder(URI flowSpecUri) {
    return new FlowSpec.Builder(flowSpecUri);
  }

  public static FlowSpec.Builder builder(String flowSpecUri) {
    return new FlowSpec.Builder(flowSpecUri);
  }

  public static FlowSpec.Builder builder() {
    return new FlowSpec.Builder();
  }

  /** Creates a builder for the FlowSpec based on values in a flow properties config. */
  public static FlowSpec.Builder builder(URI catalogURI, Properties flowProps) {
    String name = flowProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY);
    String group = flowProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, "default");

    try {
      URI flowURI = new URI(catalogURI.getScheme(), catalogURI.getAuthority(),
          "/" + group + "/" + name, null);
      FlowSpec.Builder builder = new FlowSpec.Builder(flowURI).withConfigAsProperties(flowProps);
      String descr = flowProps.getProperty(ConfigurationKeys.FLOW_DESCRIPTION_KEY, null);
      if (null != descr) {
        builder = builder.withDescription(descr);
      }

      return builder;
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to create a FlowSpec URI: " + e, e);
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
   * Builder for {@link FlowSpec}s.
   * <p> Defaults/conventions:
   * <ul>
   *  <li> Default flowCatalogURI is {@link #DEFAULT_FLOW_CATALOG_SCHEME}:
   *  <li> Convention for FlowSpec URI: <flowCatalogURI>/config.get({@link ConfigurationKeys#FLOW_GROUP_KEY})/config.get({@link ConfigurationKeys#FLOW_NAME_KEY})
   *  <li> Convention for Description: config.get({@link ConfigurationKeys#FLOW_DESCRIPTION_KEY})
   *  <li> Default version: empty
   * </ul>
   */
  public static class Builder {
    public static final String DEFAULT_FLOW_CATALOG_SCHEME = "gobblin-flow";
    public static final String DEFAULT_VERSION = "";
    @VisibleForTesting
    private Optional<Config> config = Optional.absent();
    private Optional<Properties> configAsProperties = Optional.absent();
    private Optional<URI> uri;
    private String version = FlowSpec.Builder.DEFAULT_VERSION;
    private Optional<String> description = Optional.absent();
    private Optional<URI> flowCatalogURI = Optional.absent();
    private Optional<Set<URI>> templateURIs = Optional.absent();
    private Optional<List<Spec>> childSpecs = Optional.absent();

    public Builder(URI flowSpecUri) {
      Preconditions.checkNotNull(flowSpecUri);
      this.uri = Optional.of(flowSpecUri);
    }

    public Builder(String flowSpecUri) {
      Preconditions.checkNotNull(flowSpecUri);
      Preconditions.checkNotNull(flowSpecUri);
      try {
        this.uri = Optional.of(new URI(flowSpecUri));
      }
      catch (URISyntaxException e) {
        throw new RuntimeException("Invalid FlowSpec config: " + e, e);
      }
    }

    public Builder() {
      this.uri = Optional.absent();
    }

    public FlowSpec build() {
      Preconditions.checkNotNull(this.uri);
      Preconditions.checkArgument(null != version, "Version should not be null");

      return new FlowSpec(getURI(), getVersion(), getDescription(), getConfig(),
          getConfigAsProperties(), getTemplateURIs(), getChildSpecs());
    }

    /** The scheme and authority of the flow catalog URI are used to generate FlowSpec URIs from
     * flow configs. */
    public FlowSpec.Builder withFlowCatalogURI(URI flowCatalogURI) {
      this.flowCatalogURI = Optional.of(flowCatalogURI);
      return this;
    }

    public FlowSpec.Builder withFlowCatalogURI(String flowCatalogURI) {
      try {
        this.flowCatalogURI = Optional.of(new URI(flowCatalogURI));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Unable to set flow catalog URI: " + e, e);
      }
      return this;
    }

    public URI getDefaultFlowCatalogURI() {
      try {
        return new URI(DEFAULT_FLOW_CATALOG_SCHEME, null, "/", null, null);
      } catch (URISyntaxException e) {
        // should not happen
        throw new Error("Unexpected exception: " + e, e);
      }
    }

    public URI getFlowCatalogURI() {
      if (! this.flowCatalogURI.isPresent()) {
        this.flowCatalogURI = Optional.of(getDefaultFlowCatalogURI());
      }
      return this.flowCatalogURI.get();
    }

    public URI getDefaultURI() {
      URI flowCatalogURI = getFlowCatalogURI();
      Config flowCfg = getConfig();
      String name = flowCfg.hasPath(ConfigurationKeys.FLOW_NAME_KEY) ?
          flowCfg.getString(ConfigurationKeys.FLOW_NAME_KEY) :
          "default";
      String group = flowCfg.hasPath(ConfigurationKeys.FLOW_GROUP_KEY) ?
          flowCfg.getString(ConfigurationKeys.FLOW_GROUP_KEY) :
          "default";
      try {
        return new URI(flowCatalogURI.getScheme(), flowCatalogURI.getAuthority(),
            "/" + group + "/" + name, null, null);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Unable to create default FlowSpec URI:" + e, e);
      }
    }

    public URI getURI() {
      if (! this.uri.isPresent()) {
        this.uri = Optional.of(getDefaultURI());
      }

      return this.uri.get();
    }


    public FlowSpec.Builder withVersion(String version) {
      Preconditions.checkNotNull(version);
      this.version = version;
      return this;
    }

    public String getVersion() {
      return this.version;
    }

    public FlowSpec.Builder withDescription(String flowDescription) {
      Preconditions.checkNotNull(flowDescription);
      this.description = Optional.of(flowDescription);
      return this;
    }

    public String getDefaultDescription() {
      Config flowConf = getConfig();
      return flowConf.hasPath(ConfigurationKeys.FLOW_DESCRIPTION_KEY) ?
          flowConf.getString(ConfigurationKeys.FLOW_DESCRIPTION_KEY) :
          "Gobblin flow " + getURI();
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

    public FlowSpec.Builder withConfig(Config flowConfig) {
      Preconditions.checkNotNull(flowConfig);
      this.config = Optional.of(flowConfig);
      return this;
    }

    public Properties getConfigAsProperties() {
      if (!this.configAsProperties.isPresent()) {
        this.configAsProperties = Optional.of(ConfigUtils.configToProperties(this.config.get()));
      }
      return this.configAsProperties.get();
    }

    public FlowSpec.Builder withConfigAsProperties(Properties flowConfig) {
      Preconditions.checkNotNull(flowConfig);
      this.configAsProperties = Optional.of(flowConfig);
      return this;
    }

    public Optional<Set<URI>> getTemplateURIs() {
      return this.templateURIs;
    }

    public FlowSpec.Builder withTemplate(URI templateURI) {
      Preconditions.checkNotNull(templateURI);
      if (!this.templateURIs.isPresent()) {
        Set<URI> templateURISet = Sets.newHashSet();
        this.templateURIs = Optional.of(templateURISet);
      }
      this.templateURIs.get().add(templateURI);
      return this;
    }

    public FlowSpec.Builder withTemplates(Collection templateURIs) {
      Preconditions.checkNotNull(templateURIs);
      if (!this.templateURIs.isPresent()) {
        Set<URI> templateURISet = Sets.newHashSet();
        this.templateURIs = Optional.of(templateURISet);
      }
      this.templateURIs.get().addAll(templateURIs);
      return this;
    }

    public Optional<List<Spec>> getChildSpecs() {
      return this.childSpecs;
    }

    public FlowSpec.Builder withChildSpec(Spec childSpec) {
      Preconditions.checkNotNull(childSpec);
      if (!this.childSpecs.isPresent()) {
        List<Spec> childSpecsList = Lists.newArrayList();
        this.childSpecs = Optional.of(childSpecsList);
      }
      this.childSpecs.get().add(childSpec);
      return this;
    }

    public FlowSpec.Builder withChildSpecs(List<Spec> childSpecs) {
      Preconditions.checkNotNull(childSpecs);
      if (!this.childSpecs.isPresent()) {
        List<Spec> childSpecsList = Lists.newArrayList();
        this.childSpecs = Optional.of(childSpecsList);
      }
      this.childSpecs.get().addAll(childSpecs);
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
