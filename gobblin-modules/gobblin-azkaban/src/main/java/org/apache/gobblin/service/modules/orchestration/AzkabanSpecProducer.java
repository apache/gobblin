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
package org.apache.gobblin.service.modules.orchestration;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzkabanSpecProducer implements SpecProducer<Spec>, Closeable {

  // Session Id for GaaS User
  private String _sessionId;
  private Config _config;

  public AzkabanSpecProducer(Config config, Optional<Logger> log) {
    this._config = config;
    try {
      // Initialize Azkaban client / producer and cache credentials
      String azkabanUsername = _config.getString(ServiceAzkabanConfigKeys.AZKABAN_USERNAME_KEY);
      String azkabanPassword = getAzkabanPassword(_config);
      String azkabanServerUrl = _config.getString(ServiceAzkabanConfigKeys.AZKABAN_SERVER_URL_KEY);

      _sessionId = AzkabanAjaxAPIClient.authenticateAndGetSessionId(azkabanUsername, azkabanPassword, azkabanServerUrl);
    } catch (IOException | EncoderException e) {
      throw new RuntimeException("Could not authenticate with Azkaban", e);
    }
  }

  private String getAzkabanPassword(Config config) {
    if (StringUtils.isNotBlank(System.getProperty(ServiceAzkabanConfigKeys.AZKABAN_PASSWORD_SYSTEM_KEY))) {
      return System.getProperty(ServiceAzkabanConfigKeys.AZKABAN_PASSWORD_SYSTEM_KEY);
    }

    return ConfigUtils.getString(config, ServiceAzkabanConfigKeys.AZKABAN_PASSWORD_KEY, StringUtils.EMPTY);
  }

  public AzkabanSpecProducer(Config config, Logger log) {
    this(config, Optional.of(log));
  }

  /** Constructor with no logging */
  public AzkabanSpecProducer(Config config) {
    this(config, Optional.<Logger>absent());
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    // If project already exists, execute it
    try {
      AzkabanProjectConfig azkabanProjectConfig = new AzkabanProjectConfig((JobSpec) addedSpec);
      boolean azkabanProjectExists = AzkabanJobHelper.isAzkabanJobPresent(_sessionId, azkabanProjectConfig);

      // If project does not already exists, create and execute it
      if (azkabanProjectExists) {
        log.info("Executing Azkaban Project: " + azkabanProjectConfig.getAzkabanProjectName());
        AzkabanJobHelper.executeJob(_sessionId, AzkabanJobHelper.getProjectId(_sessionId, azkabanProjectConfig),
            azkabanProjectConfig);
      } else {
        log.info("Setting up Azkaban Project: " + azkabanProjectConfig.getAzkabanProjectName());

        // Deleted project also returns true if-project-exists check, so optimistically first create the project
        // .. (it will create project if it was never created or deleted), if project exists it will fail with
        // .. appropriate exception message, catch that and run in replace project mode if force overwrite is
        // .. specified
        try {
          createNewAzkabanProject(_sessionId, azkabanProjectConfig);
        } catch (IOException e) {
          if ("Project already exists.".equalsIgnoreCase(e.getMessage())) {
            if (ConfigUtils.getBoolean(((JobSpec) addedSpec).getConfig(),
                ServiceAzkabanConfigKeys.AZKABAN_PROJECT_OVERWRITE_IF_EXISTS_KEY, false)) {
              log.info("Project already exists for this Spec, but force overwrite specified");
              updateExistingAzkabanProject(_sessionId, azkabanProjectConfig);
            } else {
              log.info(String.format("Azkaban project already exists: " + "%smanager?project=%s",
                  azkabanProjectConfig.getAzkabanServerUrl(), azkabanProjectConfig.getAzkabanProjectName()));
            }
          } else {
            throw e;
          }
        }
      }


    } catch (IOException e) {
      throw new RuntimeException("Issue in setting up Azkaban project.", e);
    }

    return new CompletedFuture<>(_config, null);
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    // Re-create project
    AzkabanProjectConfig azkabanProjectConfig = new AzkabanProjectConfig((JobSpec) updatedSpec);

    try {
      updateExistingAzkabanProject(_sessionId, azkabanProjectConfig);
    } catch (IOException e) {
      throw new RuntimeException("Issue in setting up Azkaban project.", e);
    }

    return new CompletedFuture<>(_config, null);
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI) {
    // Delete project
    JobSpec jobSpec = new JobSpec.Builder(deletedSpecURI).build();

    try {
      AzkabanJobHelper.deleteAzkabanJob(_sessionId, new AzkabanProjectConfig(jobSpec));
    } catch (IOException e) {
      throw new RuntimeException("Issue in deleting Azkaban project.", e);
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public Future<? extends List<Spec>> listSpecs() {
    throw new UnsupportedOperationException();
  }

  private void createNewAzkabanProject(String sessionId, AzkabanProjectConfig azkabanProjectConfig) throws IOException {
    // Create Azkaban Job
    String azkabanProjectId = AzkabanJobHelper.createAzkabanJob(sessionId, azkabanProjectConfig);

    // Schedule Azkaban Job
    AzkabanJobHelper.scheduleJob(sessionId, azkabanProjectId, azkabanProjectConfig);

    log.info(String.format("Azkaban project created: %smanager?project=%s",
        azkabanProjectConfig.getAzkabanServerUrl(), azkabanProjectConfig.getAzkabanProjectName()));
  }

  private void updateExistingAzkabanProject(String sessionId, AzkabanProjectConfig azkabanProjectConfig) throws IOException {
    log.info(String.format("Updating project: %smanager?project=%s", azkabanProjectConfig.getAzkabanServerUrl(),
        azkabanProjectConfig.getAzkabanProjectName()));

    // Get project Id
    String azkabanProjectId = AzkabanJobHelper.getProjectId(sessionId, azkabanProjectConfig);

    // Replace Azkaban Job
    AzkabanJobHelper.replaceAzkabanJob(sessionId, azkabanProjectId, azkabanProjectConfig);

    // Change schedule
    AzkabanJobHelper.changeJobSchedule(sessionId, azkabanProjectId, azkabanProjectConfig);
  }
}
