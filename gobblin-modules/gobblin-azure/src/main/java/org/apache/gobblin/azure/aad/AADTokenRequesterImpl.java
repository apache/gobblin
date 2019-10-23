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

package org.apache.gobblin.azure.aad;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Optional;
import com.microsoft.aad.adal4j.AuthenticationCallback;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ExecutorsUtils;


/**
 * An implementation of AADTokenRequester that retrieves authentication token from an Azure Active Directory
 * for a service principal.
 * <p>
 * The retrieved tokens are not cached. The tokens are cached within {@link CachedAADAuthenticator}
 */
@Slf4j
public class AADTokenRequesterImpl implements AADTokenRequester {
  /**
   * For ADF Pipeline execution, "management" is the target resource where the token will be sent to
   */
  public final static String TOKEN_TARGET_RESOURCE_MANAGEMENT = "https://management.core.windows.net/";
  public final static String TOKEN_TARGET_RESOURCE_KEY_VAULT = "https://vault.azure.net";
  /**
   * The shared thread pool used by all {@link AADTokenRequesterImpl}
   */
  private final static ExecutorService authServiceThreadPool;
  /**
   * static variable for a singleton instance
   */
  private final static AADTokenRequesterImpl instance = new AADTokenRequesterImpl();

  static {
    ThreadFactory threadFactory =
        ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of("aad-authentication-%d"));
    authServiceThreadPool = Executors.newCachedThreadPool(threadFactory);
  }

  /**
   * private constructor for this singleton class
   */
  private AADTokenRequesterImpl() {
  }

  // static method to create instance of Singleton class
  public static AADTokenRequesterImpl getInstance() {
    return instance;
  }

  /**
   * {@inheritDoc}
   */
  public AuthenticationResult getToken(AADTokenIdentifier tokenId)
      throws MalformedURLException, ExecutionException, InterruptedException {
    AuthenticationContext authContext =
        new AuthenticationContext(tokenId.getAuthorityUrl(), false, authServiceThreadPool);
    ClientCredential credential =
        new ClientCredential(tokenId.getServicePrincipalId(), tokenId.getServicePrincipalSecret());
    AuthenticationResult token =
        authContext.acquireToken(tokenId.getTargetResource(), credential, new AuthenticationCallback() {
          @Override
          public void onSuccess(Object result) {
            log.debug("Successfully got AAD authentication token: " + result.toString());
          }

          @Override
          public void onFailure(Throwable exc) {
            log.error("Failed to get AAD authentication token: " + exc.getMessage());
            throw new RuntimeException("Failed to get AAD authentication token", exc);
          }
        }).get();

    log.debug("****** Acquired Token ******");
    log.debug("Token User Info" + token.getUserInfo());
    log.debug("Token Expiration" + token.getExpiresOnDate());
    //Shouldn't save the token in the log
//    log.debug("Refresh Token" + token.getRefreshToken());
//    log.debug("Access Token: " + token.getAccessToken());
    log.debug("****** ****** ****** ****** ");

    return token;
  }
}