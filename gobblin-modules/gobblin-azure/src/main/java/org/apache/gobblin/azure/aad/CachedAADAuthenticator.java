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
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.aad.adal4j.AuthenticationResult;

import lombok.extern.slf4j.Slf4j;

import static org.apache.gobblin.azure.adf.ADFPipelineExecutionTask.AZURE_CONF_PREFIX;


/**
 * An implementation of {@link AADAuthenticator} with caching capability.
 * All {@link AADAuthenticator} instances share the same cache {@link CachedAADAuthenticator#cache}
 */
@Slf4j
public class CachedAADAuthenticator implements AADAuthenticator {
  private static final String CONF_CACHE_SIZE = AZURE_CONF_PREFIX + "aad.auth.cache.size";
  private static final String CONF_CACHE_EXPIRATION_MINUTES = AZURE_CONF_PREFIX + "aad.auth.cache.expiration";
  private static final long DEFAULT_CACHE_SIZE = 100;
  private static final long DEFAULT_CACHE_EXPIRATION_MINUTES = 60;

  /**
   * URL of the authenticating authority for a specific Azure Active Directory
   */
  private static final String AUTHORITY_URL_PATTERN = "https://login.microsoftonline.com/%s";

  /**
   * Build a cache for all CachedAADAuthenticator instances. It relies on a {@link AADTokenRequester} to get AAD tokens
   */
  private static LoadingCache<AADTokenIdentifier, AuthenticationResult> cache;
  private final String authorityUri;
  private final AADTokenRequester aadTokenRequester;

  /**
   * @param authorityUri the full authority uri, which is of the pattern "https://login.microsoftonline.com/<aad_id>"
   * @param properties the configuration properties for the token cache
   */
  public CachedAADAuthenticator(final String authorityUri, Properties properties) {
    this(AADTokenRequesterImpl.getInstance(), authorityUri, properties);
  }

  /**
   * @param aadTokenRequester the underlying token requester
   * @param authorityUri the full authority uri, which is of the pattern "https://login.microsoftonline.com/<aad_id>"
   * @param properties the configuration properties for the token cache
   */
  public CachedAADAuthenticator(final AADTokenRequester aadTokenRequester, final String authorityUri,
      final Properties properties) {
    this.authorityUri = authorityUri;
    this.aadTokenRequester = aadTokenRequester;
    createCache(aadTokenRequester, properties);
  }

  /**
   * @param aadId the azure active directory id
   * @param properties the configuration properties for the token cache
   * @return an AADAuthenticatorImpl for your AD
   */
  public static CachedAADAuthenticator buildWithAADId(String aadId, Properties properties) {
    return new CachedAADAuthenticator(String.format(AUTHORITY_URL_PATTERN, aadId), properties);
  }

  @Override
  public String getAuthorityUri() {
    return this.authorityUri;
  }

  @Override
  public AADTokenRequester getTokenRequester() {
    return this.aadTokenRequester;
  }

  private synchronized static void createCache(final AADTokenRequester aadTokenRequester, Properties properties) {
    if (cache == null) {
      cache = createTokenCache(aadTokenRequester, properties);
    }
  }

  /**
   * @param aadTokenRequester the token requester that retrieves tokens to be kept in the cache
   * @param properties the configurations for the cache
   * @return a cache based on a {@link AADTokenRequester} instance
   */
  private static LoadingCache<AADTokenIdentifier, AuthenticationResult> createTokenCache(
      AADTokenRequester aadTokenRequester, Properties properties) {
    String cacheSizeProp = properties.getProperty(CONF_CACHE_SIZE, Long.toString(DEFAULT_CACHE_SIZE));
    Long cacheSize = Long.valueOf(cacheSizeProp);
    log.info("Creating the token cache with cache size: " + cacheSize);
    String expirationMinuteProp =
        properties.getProperty(CONF_CACHE_EXPIRATION_MINUTES, Long.toString(DEFAULT_CACHE_EXPIRATION_MINUTES));
    Long expirationMinutes = Long.valueOf(expirationMinuteProp);
    log.info("Creating the token cache with expiration minutes: " + expirationMinutes);

    return CacheBuilder.newBuilder().maximumSize(cacheSize)
        .expireAfterWrite(expirationMinutes, TimeUnit.MINUTES)
        .build(new CacheLoader<AADTokenIdentifier, AuthenticationResult>() {
          @Override
          public AuthenticationResult load(AADTokenIdentifier cacheKey)
              throws Exception {
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
   * {@inheritDoc}
   */
  @Override
  public AuthenticationResult getToken(String targetResource, String servicePrincipalId, String servicePrincipalSecret)
      throws MalformedURLException, ExecutionException, InterruptedException {
    AADTokenIdentifier key =
        new AADTokenIdentifier(getAuthorityUri(), targetResource, servicePrincipalId, servicePrincipalSecret);
    return getToken(key);
  }

  private AuthenticationResult getToken(AADTokenIdentifier key)
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
      Throwables.propagateIfInstanceOf(cause, MalformedURLException.class);
      Throwables.propagateIfInstanceOf(cause, ExecutionException.class);
      Throwables.propagateIfInstanceOf(cause, InterruptedException.class);
      //Throw other value load errors
      throw new RuntimeException(cause);
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
   * An exception to throw when there is no token retrieved for a {@link AADTokenIdentifier}
   */
  public static class NullTokenException extends Exception {
    NullTokenException(AADTokenIdentifier cacheKey) {
      super("No authentication token found for the key " + cacheKey);
    }
  }
}
