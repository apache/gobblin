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

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.aad.adal4j.AuthenticationResult;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link AADAuthenticator} with caching capability.
 * All {@link AADAuthenticator} instances share the same cache {@link CachedAADAuthenticator#cache}
 */
@Slf4j
public class CachedAADAuthenticator implements AADAuthenticator {
  private static final long CACHE_SIZE = 100;
  /**
   * Build a cache for all CachedAADAuthenticator instances
   */
  private static LoadingCache<CacheKey, AuthenticationResult> cache = createTokenCache(AADTokenRequesterImpl.getInstance());
  private final String authorityUri;

  /**
   * @param aadTokenRequester the token requester that retrieves tokens to be kept in the cache
   * @return a cache based on a {@link AADTokenRequester} instance
   */
  private static LoadingCache<CacheKey, AuthenticationResult> createTokenCache(AADTokenRequester aadTokenRequester) {
    return CacheBuilder.newBuilder().maximumSize(CACHE_SIZE)
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .build(new CacheLoader<CacheKey, AuthenticationResult>() {
          @Override
          public AuthenticationResult load(CacheKey cacheKey) throws Exception {
            log.info("Authenticate against AAD for " + cacheKey);
            AuthenticationResult token = aadTokenRequester.getToken(cacheKey);
            if (token != null) {
              return token;
            }
            throw new NullTokenException(cacheKey);
          }
        });
  }

  /**
   * @param authorityUri the full authority uri, which is of the pattern "https://login.microsoftonline.com/<aad_id>"
   */
  public CachedAADAuthenticator(final String authorityUri) {
    this.authorityUri = authorityUri;
  }

  /**
   * For test only
   */
  @VisibleForTesting
  CachedAADAuthenticator(final AADTokenRequester aadTokenRequester, final String authorityUri) {
    this.authorityUri = authorityUri;
    this.cache = createTokenCache(aadTokenRequester);
  }

  /**
   * @param aadId the azure active directory id
   * @return an AADAuthenticatorImpl for your AD
   */
  public static CachedAADAuthenticator buildWithAADId(String aadId) {
    return new CachedAADAuthenticator(String.format("https://login.microsoftonline.com/%s", aadId));
  }

  @Override
  public String getAuthorityUri() {
    return this.authorityUri;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AuthenticationResult getToken(String targetResource, String servicePrincipalId, String servicePrincipalSecret)
      throws MalformedURLException, ExecutionException, InterruptedException {
    CacheKey key = new CacheKey(getAuthorityUri(), targetResource, servicePrincipalId, servicePrincipalSecret);
    return getToken(key);
  }

  private AuthenticationResult getToken(CacheKey key)
      throws MalformedURLException, ExecutionException, InterruptedException {
    AuthenticationResult token;
    try {
      log.info("Getting token for " + key);
      token = cache.get(key);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NullTokenException) {
        //No value found for the key
        log.warn("No token found for " + key);
        return null;
      }
      //Restore the original cause while value loading
      else if (cause instanceof MalformedURLException) {
        throw (MalformedURLException) cause;
      } else if (cause instanceof ExecutionException) {
        throw (ExecutionException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      }
      //Throw other value load errors
      throw e;
    }
    //fetched a token, it won't be null here
    if (System.currentTimeMillis() >= token.getExpiresOnDate().getTime()) {
      /**
       * The token has expired, need to invalidate the token from the cache
       * Even though the cache itself has a expiration time set, we still need to validate all tokens fetched from the cache
       */
      log.info(String.format("The token for %s has expired, will retrieve again.", key));
      cache.invalidate(key);
      return getToken(key);
    }
    //return the unexpired token
    return token;
  }

  /**
   * The key that uniquely identifies a token
   */
  @AllArgsConstructor
  @Getter
  @EqualsAndHashCode
  public static class CacheKey {
    //an azure active directory authority url of the pattern "https://login.microsoftonline.com/<aad_id>"
    private final String authorityUrl;
    //identifier of the target resource that is the recipient of the requested token
    private final String targetResource;
    //the service principal id
    private final String servicePrincipalId;
    //the service principal secret
    private final String servicePrincipalSecret;

    @Override
    public String toString() {
      /**
       * Avoid printing the service principal's secret in the log
       */
      return String.format("%s{authorityUrl='%s', targetResource='%s', servicePrincipalId='%s'}",
          CacheKey.class.getSimpleName(), authorityUrl, targetResource, servicePrincipalId);
    }
  }

  /**
   * An exception to throw when there is no token retrieved for a {@link CacheKey}
   */
  public static class NullTokenException extends Exception {
    NullTokenException(CacheKey cacheKey) {
      super("No authentication token found for the key " + cacheKey);
    }
  }
}
