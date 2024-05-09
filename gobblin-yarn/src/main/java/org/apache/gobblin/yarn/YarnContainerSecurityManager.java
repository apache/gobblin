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

package org.apache.gobblin.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import java.io.IOException;

import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.logs.LogCopier;
import org.apache.gobblin.yarn.event.DelegationTokenUpdatedEvent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class for managing token renewing in the containers including the container for the
 * {@link GobblinApplicationMaster}.
 *
 * <p>
 *   This class implements a simple monitor for modifications on the token file and reloads tokens
 *   in the token file if the file has been modified and adds the tokens to the credentials of the
 *   current login user.
 * </p>
 *
 * @author Yinan Li
 */
public class YarnContainerSecurityManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnContainerSecurityManager.class);

  private final FileSystem fs;
  private final Path tokenFilePath;
  private final EventBus eventBus;
  private final LogCopier logCopier;

  public YarnContainerSecurityManager(Config config, FileSystem fs, EventBus eventBus) {
    this(config, fs, eventBus, null);
  }

  public YarnContainerSecurityManager(Config config, FileSystem fs, EventBus eventBus, LogCopier logCopier) {
    this.fs = fs;
    this.tokenFilePath = getYarnTokenFilePath(config, fs);
    this.eventBus = eventBus;
    this.logCopier = logCopier;
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleTokenFileUpdatedEvent(DelegationTokenUpdatedEvent delegationTokenUpdatedEvent) {
    try {
      addCredentials(readCredentials(this.tokenFilePath));
      if (this.logCopier != null) {
        this.logCopier.setNeedToUpdateDestFs(true);
        this.logCopier.setNeedToUpdateSrcFs(true);
      }
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  @Override
  protected void startUp() throws Exception {
    this.eventBus.register(this);
  }

  @Override
  protected void shutDown() throws Exception {
    // Nothing to do
    LOGGER.info("Attempt to shut down YarnContainerSecurityManager");
  }

  /**
   * Read the {@link Token}s stored in the token file.
   */
  @VisibleForTesting
  Credentials readCredentials(Path tokenFilePath) throws IOException {
    LOGGER.info("Reading updated credentials from token file: " + tokenFilePath);
    return Credentials.readTokenStorageFile(tokenFilePath, this.fs.getConf());
  }

  @VisibleForTesting
  void addCredentials(Credentials credentials) throws IOException {
    for (Token<? extends TokenIdentifier> token : credentials.getAllTokens()) {
      LOGGER.info("updating " + token.toString());
    }
    UserGroupInformation.getCurrentUser().addCredentials(credentials);
  }

  /**
   * A utility method to get the location of the generated security token
   * @param config - the configuration that contains the application name and the token file path
   * @param fs - the Filesystem that stores the security token
   * @return the path to the security token
   */
  static Path getYarnTokenFilePath(Config config, FileSystem fs) {
    if (config.hasPath(GobblinYarnConfigurationKeys.TOKEN_FILE_PATH_KEY)) {
      return new Path(config.getString(GobblinYarnConfigurationKeys.TOKEN_FILE_PATH_KEY), GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);
    }
    // Default to storing the token file in the home directory of the user
    return new Path(fs.getHomeDirectory(), PathUtils.combinePaths(config.getString(GobblinYarnConfigurationKeys.APPLICATION_NAME_KEY),
        GobblinYarnConfigurationKeys.TOKEN_FILE_NAME));
  }
}
