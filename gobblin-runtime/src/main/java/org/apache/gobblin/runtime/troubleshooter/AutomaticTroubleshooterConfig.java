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

package org.apache.gobblin.runtime.troubleshooter;

import java.util.Objects;

import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * See documentation in {@link ConfigurationKeys} for explanation of settings.
 * */
@Getter
@Builder
@AllArgsConstructor
public class AutomaticTroubleshooterConfig {
  private final boolean disabled;
  private final boolean disableEventReporting;

  @Builder.Default
  private int inMemoryRepositoryMaxSize = ConfigurationKeys.DEFAULT_TROUBLESHOOTER_IN_MEMORY_ISSUE_REPOSITORY_MAX_SIZE;

  @Inject
  public AutomaticTroubleshooterConfig(Config config) {
    Objects.requireNonNull(config, "Config cannot be null");

    disabled = ConfigUtils.getBoolean(config, ConfigurationKeys.TROUBLESHOOTER_DISABLED, false);
    disableEventReporting =
        ConfigUtils.getBoolean(config, ConfigurationKeys.TROUBLESHOOTER_DISABLE_EVENT_REPORTING, false);

    inMemoryRepositoryMaxSize = ConfigUtils
        .getInt(config, ConfigurationKeys.TROUBLESHOOTER_IN_MEMORY_ISSUE_REPOSITORY_MAX_SIZE,
                ConfigurationKeys.DEFAULT_TROUBLESHOOTER_IN_MEMORY_ISSUE_REPOSITORY_MAX_SIZE);
  }
}
