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
 * A class that authenticates a service principal against an Azure Active Directory through {@link AADTokenRequester}
 */
public interface AADAuthenticator {
  /**
   * @return the full authority uri for your azure active directory
   * the pattern "https://login.microsoftonline.com/<aad_id>"
   */
  String getAuthorityUri();

  /**
   * @return the {@link AADTokenRequester} who does the actual token request
   */
  AADTokenRequester getTokenRequester();

  /**
   * Fetch an authentication token from Azure Active Directory.
   * The job will be dispatched to the underlying {@link AADTokenRequester}
   *
   * @param targetResource         identifier of the target resource that is the recipient of the requested token
   * @param servicePrincipalId     the service principal id
   * @param servicePrincipalSecret the service principal secret
   * @return the authentication token for the service principal described in the key
   * @throws MalformedURLException thrown if URL is invalid
   * @throws ExecutionException    if the AuthenticationResult computation threw an exception
   * @throws InterruptedException  if the AuthenticationResult was interrupted while waiting
   */
  AuthenticationResult getToken(String targetResource, String servicePrincipalId, String servicePrincipalSecret)
      throws MalformedURLException, ExecutionException, InterruptedException;
}
