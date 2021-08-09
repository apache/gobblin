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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.template.ResourceBasedJobTemplate;
import org.apache.gobblin.runtime.template.StaticJobTemplate;
import org.apache.gobblin.util.ConfigUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


@Slf4j
/**
 * Defines a Gobblin Job that can be run once, or multiple times. A {@link JobSpec} is
 * {@link Configurable} so it has an associated {@link Config}, along with other mandatory
 * properties such as a uri, description, and version. A {@link JobSpec} is
 * uniquely identified by its uri (containing group and name).
 *
 */
@Alpha
@Data
@AllArgsConstructor
public class JobSpec implements Configurable, Spec {
  private static final long serialVersionUID = 6074793380396465963L;

  /** An URI identifying the job. */
  URI uri;
  /** The implementation-defined version of this spec. */
  String version;
  /** Human-readable description of the job spec */
  String description;
  /** Job config as a typesafe config object*/
  Config config;
  /** Job config as a properties collection for backwards compatibility */
  // Note that this property is not strictly necessary as it can be generated from the typesafe
  // config. We use it as a cache until typesafe config is more widely adopted in Gobblin.
  Properties configAsProperties;
  /** URI of {@link org.apache.gobblin.runtime.api.JobTemplate} to use. */
  Optional<URI> templateURI;
  /** Specific instance of template. */
  transient Optional<JobTemplate> jobTemplate;

  /** Metadata can contain properties which are not a part of config, e.g. Verb */
  Map<String, String> metadata;

  /** A Verb identifies if the Spec is for Insert/Update/Delete */
  private static final String IN_MEMORY_TEMPLATE_URI = "inmemory";

  public static Builder builder(URI jobSpecUri) {
    return new Builder(jobSpecUri);
  }

  public static Builder builder(String jobSpecUri) {
    return new Builder(jobSpecUri);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Creates a builder for the JobSpec based on values in a job properties config. */
  public static Builder builder(URI catalogURI, Properties jobProps) {
    String name = JobState.getJobNameFromProps(jobProps);
    String group = JobState.getJobGroupFromProps(jobProps);
    if (null == group) {
      group = "default";
    }
    try {
      URI jobURI = new URI(catalogURI.getScheme(), catalogURI.getAuthority(),
          "/" + group + "/" + name, null);
      Builder builder = new Builder(jobURI).withConfigAsProperties(jobProps);
      String descr = JobState.getJobDescriptionFromProps(jobProps);
      if (null != descr) {
        builder.withDescription(descr);
      }

      return builder;
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to create a JobSpec URI: " + e, e);
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
   * Builder for {@link JobSpec}s.
   * <p> Defaults/conventions:
   * <ul>
   *  <li> Default jobCatalogURI is {@link #DEFAULT_JOB_CATALOG_SCHEME}:
   *  <li> Convention for JobSpec URI: <jobCatalogURI>/config.get({@link ConfigurationKeys#JOB_GROUP_KEY})/config.get({@link ConfigurationKeys#JOB_NAME_KEY})
   *  <li> Convention for Description: config.get({@link ConfigurationKeys#JOB_DESCRIPTION_KEY})
   *  <li> Default version: 1
   * </ul>
   */
  public static class Builder {
    public static final String DEFAULT_JOB_CATALOG_SCHEME = "gobblin-job";
    @VisibleForTesting
    private Optional<Config> config = Optional.absent();
    private Optional<Properties> configAsProperties = Optional.absent();
    private Optional<URI> uri;
    private String version = "1";
    private Optional<String> description = Optional.absent();
    private Optional<URI> jobCatalogURI = Optional.absent();
    private Optional<URI> templateURI = Optional.absent();
    private Optional<JobTemplate> jobTemplate = Optional.absent();
    private Optional<Map> metadata = Optional.absent();

    public Builder(URI jobSpecUri) {
      Preconditions.checkNotNull(jobSpecUri);
      this.uri = Optional.of(jobSpecUri);
    }

    public Builder(String jobSpecUri) {
      Preconditions.checkNotNull(jobSpecUri);
      Preconditions.checkNotNull(jobSpecUri);
      try {
        this.uri = Optional.of(new URI(jobSpecUri));
      }
      catch (URISyntaxException e) {
        throw new RuntimeException("Invalid JobSpec config: " + e, e);
      }
    }

    public Builder() {
      this.uri = Optional.absent();
    }

    public JobSpec build() {
      Preconditions.checkNotNull(this.uri);
      Preconditions.checkNotNull(this.version);
      return new JobSpec(getURI(), getVersion(), getDescription(), getConfig(),
                         getConfigAsProperties(), getTemplateURI(), getTemplate(), getMetadata());
    }

    /** The scheme and authority of the job catalog URI are used to generate JobSpec URIs from
     * job configs. */
    public Builder withJobCatalogURI(URI jobCatalogURI) {
      this.jobCatalogURI = Optional.of(jobCatalogURI);
      return this;
    }

    public Builder withJobCatalogURI(String jobCatalogURI) {
      try {
        this.jobCatalogURI = Optional.of(new URI(jobCatalogURI));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Unable to set job catalog URI: " + e, e);
      }
      return this;
    }

    public URI getDefaultJobCatalogURI() {
      try {
        return new URI(DEFAULT_JOB_CATALOG_SCHEME, null, "/", null, null);
      } catch (URISyntaxException e) {
        // should not happen
        throw new Error("Unexpected exception: " + e, e);
      }
    }

    public URI getJobCatalogURI() {
      if (! this.jobCatalogURI.isPresent()) {
        this.jobCatalogURI = Optional.of(getDefaultJobCatalogURI());
      }
      return this.jobCatalogURI.get();
    }

    public URI getDefaultURI() {
      URI jobCatalogURI = getJobCatalogURI();
      Config jobCfg = getConfig();
      String name = jobCfg.hasPath(ConfigurationKeys.JOB_NAME_KEY) ?
          jobCfg.getString(ConfigurationKeys.JOB_NAME_KEY) :
          "default";
      String group = jobCfg.hasPath(ConfigurationKeys.JOB_GROUP_KEY) ?
          jobCfg.getString(ConfigurationKeys.JOB_GROUP_KEY) :
          "default";
      try {
        return new URI(jobCatalogURI.getScheme(), jobCatalogURI.getAuthority(),
                       "/" + group + "/" + name, null, null);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Unable to create default JobSpec URI:" + e, e);
      }
    }

    public URI getURI() {
      if (! this.uri.isPresent()) {
        this.uri = Optional.of(getDefaultURI());
      }

      return this.uri.get();
    }


    public Builder withVersion(String version) {
      Preconditions.checkNotNull(version);
      this.version = version;
      return this;
    }

    public String getVersion() {
      return this.version;
    }

    public Builder withDescription(String jobDescription) {
      Preconditions.checkNotNull(jobDescription);
      this.description = Optional.of(jobDescription);
      return this;
    }

    public String getDefaultDescription() {
      Config jobConf = getConfig();
      return jobConf.hasPath(ConfigurationKeys.JOB_DESCRIPTION_KEY) ?
          jobConf.getString(ConfigurationKeys.JOB_DESCRIPTION_KEY) :
          "Gobblin job " + getURI();
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

    public Builder withConfig(Config jobConfig) {
      Preconditions.checkNotNull(jobConfig);
      this.config = Optional.of(jobConfig);
      return this;
    }

    public Properties getConfigAsProperties() {
      if (!this.configAsProperties.isPresent()) {
        this.configAsProperties = Optional.of(ConfigUtils.configToProperties(this.config.get()));
      }
      return this.configAsProperties.get();
    }

    public Builder withConfigAsProperties(Properties jobConfig) {
      Preconditions.checkNotNull(jobConfig);
      this.configAsProperties = Optional.of(jobConfig);
      return this;
    }

    public Optional<URI> getTemplateURI() {
      return this.templateURI;
    }

    public Builder withTemplate(URI templateURI) {
      Preconditions.checkNotNull(templateURI);
      this.templateURI = Optional.of(templateURI);
      return this;
    }

    /**
     * As the public interface of {@link JobSpec} doesn't really support multiple templates,
     * for incoming list of template uris we will consolidate them as well as resolving. The resolved Config
     * will not be materialized but reside only in memory through the lifecycle.
     *
     * Restriction: This method assumes no customized jobTemplateCatalog and uses classpath resources as the input.
     * Also, the order of list matters: The former one will be overwritten by the latter.
     */
    public Builder withResourceTemplates(List<URI> templateURIs) {
      try {
        List<JobTemplate> templates = new ArrayList<>(templateURIs.size());
        for(URI uri : templateURIs) {
          templates.add(ResourceBasedJobTemplate.forResourcePath(uri.getPath()));
        }
        this.jobTemplate = Optional.of(new StaticJobTemplate(new URI(IN_MEMORY_TEMPLATE_URI), "", "",
            ConfigFactory.empty(), templates));

      } catch (URISyntaxException | SpecNotFoundException | JobTemplate.TemplateException | IOException e) {
        throw new RuntimeException("Fatal exception: Templates couldn't be resolved properly, ", e);
      }
      return this;
    }

    public Optional<JobTemplate> getTemplate() {
      return this.jobTemplate;
    }

    public Builder withTemplate(JobTemplate template) {
      Preconditions.checkNotNull(template);
      this.jobTemplate = Optional.of(template);
      return this;
    }

    public Map getDefaultMetadata() {
      log.debug("Job Spec Verb is not provided, using type 'UNKNOWN'.");
      return ImmutableMap.of(SpecExecutor.VERB_KEY, SpecExecutor.Verb.UNKNOWN.name());
    }

    public Map getMetadata() {
      if (!this.metadata.isPresent()) {
        this.metadata = Optional.of(getDefaultMetadata());
      }
      return this.metadata.get();
    }

    public Builder withMetadata(Map<String, String> metadata) {
      Preconditions.checkNotNull(metadata);
      this.metadata = Optional.of(metadata);
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

  private void writeObject(java.io.ObjectOutputStream stream)
      throws IOException {
    stream.writeObject(uri);
    stream.writeObject(version);
    stream.writeObject(description);
    stream.writeObject(templateURI.isPresent() ? templateURI.get() : null);
    stream.writeObject(configAsProperties);
  }

  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    uri = (URI) stream.readObject();
    version = (String) stream.readObject();
    description = (String) stream.readObject();
    templateURI = Optional.fromNullable((URI) stream.readObject());
    configAsProperties = (Properties) stream.readObject();
    config = ConfigUtils.propertiesToConfig(configAsProperties);
  }
}
