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
import java.util.Arrays;
import java.util.concurrent.ScheduledFuture;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.hadoop.TokenUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.helix.HelixManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;


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

  public YarnAppSecurityManagerWithKeytabs(Config config, HelixManager helixManager, FileSystem fs, Path tokenFilePath)
      throws IOException {
    super(config, helixManager, fs, tokenFilePath);
    this.loginUser = UserGroupInformation.getLoginUser();
  }

  /**
   * Renew the existing delegation token.
   */
  protected synchronized void renewDelegationToken() throws IOException, InterruptedException {
    LOGGER.debug("renewing all tokens {}", credentials.getAllTokens());

    credentials.getAllTokens().forEach(
      existingToken -> {
        try {
          long expiryTime = existingToken.renew(this.fs.getConf());
          LOGGER.info("renewed token: {}, expiryTime: {}, Id; {}", existingToken, expiryTime, Arrays.toString(existingToken.getIdentifier()));

          // TODO: If token failed to get renewed in case its expired ( can be detected via the error text ),
          //  it should call the login() to reissue the new tokens
        } catch (IOException | InterruptedException e) {
          LOGGER.error("Error renewing token: " + existingToken + " ,error: " + e, e);
        }
      }
    );

    writeDelegationTokenToFile(credentials);

    if (!this.firstLogin) {
      LOGGER.info("This is not a first login, sending TokenFileUpdatedMessage.");
      sendTokenFileUpdatedMessage();
    } else {
      LOGGER.info("This is first login of the interval, so skipping sending TokenFileUpdatedMessage.");
    }
  }

  /**
   * Get a new delegation token for the current logged-in user.
   */
  @VisibleForTesting
  synchronized void getNewDelegationTokenForLoginUser() throws IOException, InterruptedException {
    final Configuration newConfig = new Configuration();
    final Credentials allCreds = new Credentials();
//    Text renewer = TokenUtils.getMRTokenRenewerInternal(new JobConf());
    String renewer = UserGroupInformation.getLoginUser().getShortUserName();

    LOGGER.info("creating new login tokens with renewer: {}", renewer);
    TokenUtils.getAllFSTokens(newConfig, allCreds, renewer, Optional.absent(), ConfigUtils.getStringList(this.config, TokenUtils.OTHER_NAMENODES));
    //TODO: Any other required tokens can be fetched here based on config or any other detection mechanism

    LOGGER.debug("All new tokens in credential: {}", allCreds.getAllTokens());

    this.credentials = allCreds;
  }

  /**
   * Login the user from a given keytab file.
   */
  protected synchronized void login() throws IOException, InterruptedException {
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
    LOGGER.info("Login using kerberos principal : "+ principal);

    Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, UserGroupInformation.AuthenticationMethod.KERBEROS.toString().toLowerCase());
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keyTabFilePath);

    LOGGER.info(String.format("Logged in from keytab file %s using principal %s for user: %s", keyTabFilePath, principal, this.loginUser));

    getNewDelegationTokenForLoginUser();

    writeDelegationTokenToFile(this.credentials);

    UserGroupInformation.getCurrentUser().addCredentials(this.credentials);

    if (!this.firstLogin) {
      LOGGER.info("This is not a first login, sending TokenFileUpdatedMessage from Login().");
      sendTokenFileUpdatedMessage();
    }else {
      LOGGER.info("This is first login of the interval, so skipping sending TokenFileUpdatedMessage from Login().");
    }
  }
}
