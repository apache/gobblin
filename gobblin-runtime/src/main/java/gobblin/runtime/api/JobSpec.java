/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.api;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alpha;
import gobblin.util.ConfigUtils;

import lombok.Data;


/**
 * Defines a Gobblin Job that can be run once, or multiple times. A {@link JobSpec} is
 * {@link Configurable} so it has an associated {@link Config}, along with other mandatory
 * properties such as a uri, description, and version. A {@link JobSpec} is
 * uniquely identified by its uri (containing group and name).
 *
 */
@Alpha
@Data
public class JobSpec implements Configurable {
  /** An URI identifying the job. */
  final URI uri;
  /** The implementation-defined version of this spec. */
  final String version;
  /** Human-readable description of the job spec */
  final String description;
  /** Job config as a typesafe config object*/
  final Config config;
  /** Job config as a properties collection for backwards compatibility */
  // Note that this property is not strictly necessary as it can be generated from the typesafe
  // config. We use it as a cache until typesafe config is more widely adopted in Gobblin.
  final Properties configAsProperties;

  public static Builder builer(URI jobSpecUri) {
    return new Builder(jobSpecUri);
  }

  public static Builder builer(String jobSpecUri) {
    return new Builder(jobSpecUri);
  }

  public static class Builder {
    private Optional<Config> config = Optional.absent();
    private Optional<Properties> configAsProperties = Optional.absent();
    private URI uri;
    private String version = "1";
    private Optional<String> description = Optional.absent();

    public Builder(URI jobSpecUri) {
      Preconditions.checkNotNull(jobSpecUri);
      this.uri = jobSpecUri;
    }

    public Builder(String jobSpecUri) {
      Preconditions.checkNotNull(jobSpecUri);
      Preconditions.checkNotNull(jobSpecUri);
      try {
        this.uri = new URI(jobSpecUri);
      }
      catch (URISyntaxException e) {
        throw new RuntimeException("Invalid JobSpec config: " + e, e);
      }
    }

    public JobSpec build() {
      Preconditions.checkNotNull(this.uri);
      Preconditions.checkNotNull(this.version);

      if (! this.config.isPresent() && ! this.configAsProperties.isPresent()) {
        this.config = Optional.of(ConfigFactory.empty());
      }
      if (! this.configAsProperties.isPresent()) {
        this.configAsProperties = Optional.of(ConfigUtils.configToProperties(this.config.get()));
      }
      if (! this.config.isPresent()) {
        this.config = Optional.of(ConfigUtils.propertiesToConfig(this.configAsProperties.get()));
      }
      if (! this.description.isPresent()) {
        this.description = Optional.of("Gobblin job " + this.uri);
      }
      return new JobSpec(this.uri, this.version, this.description.get(), this.config.get(),
          this.configAsProperties.get());
    }

    public Builder withVersion(String version) {
      Preconditions.checkNotNull(version);
      this.version = version;
      return this;
    }

    public Builder withDescription(String jobDescription) {
      Preconditions.checkNotNull(jobDescription);
      this.description = Optional.of(jobDescription);
      return this;
    }

    public Builder withConfig(Config jobConfig) {
      Preconditions.checkNotNull(jobConfig);
      this.config = Optional.of(jobConfig);
      return this;
    }

    public Builder withConfigAsProperties(Properties jobConfig) {
      Preconditions.checkNotNull(jobConfig);
      this.configAsProperties = Optional.of(jobConfig);
      return this;
    }
  }
}
