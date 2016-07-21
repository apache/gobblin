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
import java.util.Optional;
import java.util.Properties;

import com.typesafe.config.Config;

import gobblin.annotation.Alpha;

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
  /** Job config as a typesafe config object*/
  final Config config;
  /** Job config as a properties collection for backwards compatibility */
  // Note that this property is not strictly necessary as it can be generated from the typesafe
  // config. We use it as a cache until typesafe config is more widely adopted in Gobblin.
  final Properties configAsProperties;
  /** An URI identifying the job. */
  final URI uri;
  /** The implementation-defined version of this spec. */
  final String version;
  /** Human-readable description of the job spec */
  final String description;
  /** The URI of a optional job template used to generate the spec */
  final Optional<URI> template;
}
