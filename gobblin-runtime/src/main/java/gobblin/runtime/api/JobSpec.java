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

import gobblin.annotation.Alpha;

/**
 * Defines a Gobblin Job that can be run once, or multiple times. A {@link JobSpec} is
 * {@link Configurable} so it has an associated {@link Config}, along with other mandatory
 * properties such as a uri, description, and version. A {@link JobSpec} is
 * uniquely identified by its uri (containing group and name).
 *
 */
@Alpha
public interface JobSpec extends Configurable {
  /** An URI identifying the job. */
  URI getUri();
  /** The implementation-defined version of this spec. */
  String getVersion();
  /** Human-readable description of the job spec */
  String getDescription();
  /** The URI of a optional job template used to generate the spec */
  Optional<URI> getTemplate();
}
