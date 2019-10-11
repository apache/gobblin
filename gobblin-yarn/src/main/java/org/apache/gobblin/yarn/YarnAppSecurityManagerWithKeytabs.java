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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import com.typesafe.config.Config;


/**
 * A class for managing Kerberos login and token renewing on the client side that has access to
 * the keytab file.
 *
 * <p>
 *   This class works with {@link YarnContainerSecurityManager} to manage renewing of delegation
 *   tokens across the application. This class is responsible for login through a Kerberos keytab,
 *   renewing the delegation token, and storing the token to a token file on HDFS. It sends a
 *   Helix message to the controller and all the participants upon writing the token to the token
 *   file, which rely on the {@link YarnContainerSecurityManager} to read the token in the file
 *   upon receiving the message.
 * </p>
 *
 * <p>
 *   This class uses a scheduled task to do Kerberos re-login to renew the Kerberos ticket on a
 *   configurable schedule if login is from a keytab file. It also uses a second scheduled task
 *   to renew the delegation token after each login. Both the re-login interval and the token
 *   renewing interval are configurable.
 * </p>
 *
 * @author Yinan Li
 */
public class YarnAppSecurityManagerWithKeytabs extends AbstractYarnAppSecurityManager {

  private UserGroupInformation loginUser;
  private Optional<ScheduledFuture<?>> scheduledTokenRenewTask = Optional.absent();

  // This flag is used to tell if this is the first login. If yes, no token updated message will be
  // sent to the controller and the participants as they may not be up running yet. The first login
  // happens after this class starts up so the token gets regularly refreshed before the next login.
  private volatile boolean firstLogin = true;

  public YarnAppSecurityManagerWithKeytabs(Config config, HelixManager helixManager, FileSystem fs, Path tokenFilePath)
      throws IOException {
    super(config, helixManager, fs, tokenFilePath);
    this.loginUser = UserGroupInformation.getLoginUser();
  }

  /**
   * Renew the existing delegation token.
   */
  protected synchronized void renewDelegationToken() throws IOException, InterruptedException {
    this.token.renew(this.fs.getConf());
    writeDelegationTokenToFile();

    if (!this.firstLogin) {
      // Send a message to the controller and all the participants if this is not the first login
      sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
      sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT);
    }
  }

  /**
   * Get a new delegation token for the current logged-in user.
   */
  @VisibleForTesting
  synchronized void getNewDelegationTokenForLoginUser() throws IOException {
    this.token = this.fs.getDelegationToken(this.loginUser.getShortUserName());
  }

  /**
   * Login the user from a given keytab file.
   */
  protected void login() throws IOException {
    String keyTabFilePath = this.config.getString(GobblinYarnConfigurationKeys.KEYTAB_FILE_PATH);
    if (Strings.isNullOrEmpty(keyTabFilePath)) {
      throw new IOException("Keytab file path is not defined for Kerberos login");
    }

    if (!new File(keyTabFilePath).exists()) {
      throw new IOException("Keytab file not found at: " + keyTabFilePath);
    }

    String principal = this.config.getString(GobblinYarnConfigurationKeys.KEYTAB_PRINCIPAL_NAME);
    if (Strings.isNullOrEmpty(principal)) {
      principal = this.loginUser.getShortUserName() + "/localhost@LOCALHOST";
    }

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication",
        UserGroupInformation.AuthenticationMethod.KERBEROS.toString().toLowerCase());
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keyTabFilePath);
    LOGGER.info(String.format("Logged in from keytab file %s using principal %s", keyTabFilePath, principal));

    this.loginUser = UserGroupInformation.getLoginUser();

    getNewDelegationTokenForLoginUser();
    writeDelegationTokenToFile();

    if (!this.firstLogin) {
      // Send a message to the controller and all the participants
      sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
      sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT);
    }
  }

}
