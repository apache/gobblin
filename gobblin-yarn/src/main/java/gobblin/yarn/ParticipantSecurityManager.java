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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.yarn.event.DelegationTokenUpdatedEvent;


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

  public ParticipantSecurityManager(FileSystem fs, EventBus eventBus) {
    this.fs = fs;
    eventBus.register(this);
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleTokenFileUpdatedEvent(DelegationTokenUpdatedEvent delegationTokenUpdatedEvent) {
    try {
      readAndAddDelegationTokens(new Path(delegationTokenUpdatedEvent.getTokenFilePath()));
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  @Override
  protected void startUp() throws Exception {
    // Nothing to do
  }

  @Override
  protected void shutDown() throws Exception {
    // Nothing to do
  }

  /**
   * Read the {@link Token}s stored in the token file and add them for the login user.
   */
  private void readAndAddDelegationTokens(Path tokenFilePath) throws IOException {
    Collection<Token<? extends TokenIdentifier>> tokens =
        YarnHelixUtils.readTokensFromFile(tokenFilePath, this.fs.getConf());
    for (Token<? extends TokenIdentifier> token : tokens) {
      if (!UserGroupInformation.getCurrentUser().addToken(token)) {
        LOGGER.error(String.format("Failed to add token %s to user %s",
            token.toString(), UserGroupInformation.getLoginUser().getShortUserName()));
      }
    }
  }
}
