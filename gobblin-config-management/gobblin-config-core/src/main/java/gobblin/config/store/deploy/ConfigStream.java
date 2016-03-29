/*
 * Copyright (C) 2015-16 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.config.store.deploy;

import java.io.InputStream;

import com.google.common.base.Optional;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 *  A wrapper for {@link InputStream} that also contains the path of the resource.
 */
@AllArgsConstructor
@Getter
public class ConfigStream {
  private final Optional<InputStream> inputStream;
  private final String configPath;
}
