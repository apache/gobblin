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

import com.microsoft.aad.adal4j.AuthenticationResult;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;

/**
 * The entity that sends the actual request to get AAD Token
 */
public interface AADTokenRequester {
  /**
   * @param key a {@link org.apache.gobblin.azure.aad.CachedAADAuthenticator.CacheKey} that is used to retrieve
   *            the authentication against azure active directory
   * @return the authentication token for the service principal described in the key
   * @throws MalformedURLException thrown if URL is invalid
   * @throws ExecutionException    if the AuthenticationResult computation threw an exception
   * @throws InterruptedException  if the AuthenticationResult was interrupted while waiting
   */
  AuthenticationResult getToken(CachedAADAuthenticator.CacheKey key)
      throws MalformedURLException, ExecutionException, InterruptedException;
}
