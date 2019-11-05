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

package org.apache.gobblin.service.modules.dataset;

import com.google.common.base.Enums;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Describes a dataset behind a HTTP scheme.
 * path refers to the HTTP path of a given dataset.
 * e.g, https://some-api:443/user/123/names, where /user/123/names is the path
 * query string is not supported
 */
@Slf4j
@ToString (exclude = {"rawConfig"})
@EqualsAndHashCode (exclude = {"rawConfig"}, callSuper = true)
public class HttpDatasetDescriptor extends BaseDatasetDescriptor implements DatasetDescriptor {

  @Getter
  private final String path;
  @Getter
  private final Config rawConfig;

  public enum Platform {
    HTTP("http"),
    HTTPS("https");

    private final String platform;

    Platform(final String platform) {
      this.platform = platform;
    }

    @Override
    public String toString() {
      return this.platform;
    }
  }

  public HttpDatasetDescriptor(Config config) throws IOException {
    super(config);
    if (!isPlatformValid()) {
      throw new IOException("Invalid platform specified for HttpDatasetDescriptor: " + getPlatform());
    }
    // refers to the full HTTP url
    this.path = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PATH_KEY, "");
    this.rawConfig = config.withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef(this.path)).withFallback(super.getRawConfig());
  }

  /**
   * @return true if the platform is valid, false otherwise
   */
  private boolean isPlatformValid() {
    return Enums.getIfPresent(HttpDatasetDescriptor.Platform.class, getPlatform().toUpperCase()).isPresent();
  }

  /**
   * Check if this HTTP path equals the other HTTP path
   *
   * @param other whose path should be in the format of a HTTP path
   */
  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    // Might be null
    String otherPath = other.getPath();
    return this.path.equals(otherPath);
  }
}
