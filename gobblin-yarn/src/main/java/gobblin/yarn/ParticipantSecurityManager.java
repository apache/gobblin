/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

import com.typesafe.config.Config;

import gobblin.util.ExecutorsUtils;


/**
 * A class for managing token renewing in the Participants.
 *
 * <p>
 *   This class implements a simple monitor for modifications on the token file and reloads tokens
 *   in the token file if the file has been modified and adds the tokens to the credentials of the
 *   current login user.
 * </p>
 *
 * @author ynli
 */
public class ParticipantSecurityManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParticipantSecurityManager.class);

  private final FileSystem fs;
  private final Path tokenFilePath;
  private final long tokenFileMonitorIntervalInMinutes;

  private final ScheduledExecutorService tokenFileMonitorExecutor;

  public ParticipantSecurityManager(Config config, FileSystem fs) {
    this.fs = fs;

    this.tokenFilePath = new Path(config.getString(ConfigurationConstants.TOKEN_FILE_PATH));
    this.fs.makeQualified(tokenFilePath);

    this.tokenFileMonitorIntervalInMinutes =
        config.getLong(ConfigurationConstants.TOKEN_FILE_MONITOR_INTERVAL_IN_MINUTES);

    this.tokenFileMonitorExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("KeytabReLoginExecutor")));
  }

  @Override
  protected void startUp() throws Exception {
    if (!this.fs.exists(this.tokenFilePath)) {
      throw new IOException(String.format("Token file %s does not exist", this.tokenFilePath));
    }

    // Read and add delegation tokens from the token file upon startup. The token file should exist at
    // this time because the AM initializes the token file before starting the containers/participants.
    readAndAddDelegationTokens();

    // Then schedule the task to monitor modifications on the token file
    this.tokenFileMonitorExecutor.scheduleAtFixedRate(new Runnable() {

      private long tokenFileModificationTime = -1l;

      @Override
      public void run() {
        try {
          long currentTokenFileModificationTime = getTokenFileModificationTime();
          if (currentTokenFileModificationTime != this.tokenFileModificationTime) {
            // Reload tokens from the token file if it has been modified
            readAndAddDelegationTokens();
            this.tokenFileModificationTime = currentTokenFileModificationTime;
          }
        } catch (IOException ioe) {
          LOGGER.error("Failed to ", ioe);
        }
      }
    }, this.tokenFileMonitorIntervalInMinutes, this.tokenFileMonitorIntervalInMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected void shutDown() throws Exception {
    ExecutorsUtils.shutdownExecutorService(this.tokenFileMonitorExecutor);
  }

  /**
   * Get the current modification time of the token file.
   */
  private long getTokenFileModificationTime() throws IOException {
    if (!this.fs.exists(this.tokenFilePath)) {
      return -1;
    }

    return this.fs.getFileStatus(this.tokenFilePath).getModificationTime();
  }

  /**
   * Read the {@link Token}s stored in the token file and add them for the login user.
   */
  private void readAndAddDelegationTokens() throws IOException {
    Collection<Token<? extends TokenIdentifier>> tokens =
        YarnHelixUtils.readTokensFromFile(this.tokenFilePath, this.fs.getConf());
    for (Token<? extends TokenIdentifier> token : tokens) {
      if (!UserGroupInformation.getCurrentUser().addToken(token)) {
        LOGGER.error(String.format("Failed to add token %s to user %s",
            token.toString(), UserGroupInformation.getLoginUser().getShortUserName()));
      }
    }
  }
}
