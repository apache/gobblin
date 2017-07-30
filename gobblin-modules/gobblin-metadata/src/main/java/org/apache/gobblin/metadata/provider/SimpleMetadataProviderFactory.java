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

package gobblin.metadata.provider;

import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.util.ConfigUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * Simple {@link DatasetAwareMetadataProviderFactory} that uses a user-defined permission and creates
 * {@link SimpleConfigMetadataProvider}.
 */
@Slf4j
@Alias(value = "SimpleMetadataProvider")
public class SimpleMetadataProviderFactory implements DatasetAwareMetadataProviderFactory {

  /**
   * Permission defined in config that will be used for all paths.
   */
  public static final String ALL_PERMISSOIN = "allPermission";

  @Override
  public DatasetAwareMetadataProvider createMetadataProvider(Config metaConfig) {
    String permission = ConfigUtils.getString(metaConfig, ALL_PERMISSOIN, "");
    log.info("User defined permission is: " + permission);
    return new SimpleConfigMetadataProvider(permission);
  }
}
