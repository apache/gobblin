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

package org.apache.gobblin.azure.key_vault;

import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.keyvault.KeyVaultClient;
import com.microsoft.azure.keyvault.authentication.KeyVaultCredentials;
import com.microsoft.azure.keyvault.models.SecretBundle;
import com.microsoft.rest.credentials.ServiceClientCredentials;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.azure.aad.CachedAADAuthenticator;

/**
 * A class that handles Azure Key Vault secret retrieving requests
 * The authentication is done against Azure Active Directory based on the service principal's id and secret
 */
@Slf4j
public class KeyVaultSecretRetriever {
  private final String keyVaultUrl;

  /**
   * @param keyVaultUrl the key vault url, e.g. https://chen-vault.vault.azure.net/
   */
  public KeyVaultSecretRetriever(String keyVaultUrl) {
    this.keyVaultUrl = keyVaultUrl;
  }

  /**
   * Retrieve a secret of the name {@code secretName} as the service principal identified using {@code spId} and {@code secretName}
   *
   * @param spId       the id of service principal who is able to access the key vault
   * @param spSecret   the secret of service principal who is able to access the key vault
   * @param secretName the name of the secret you want to retrieve
   * @return the fetched secret
   */
  public SecretBundle getSecret(String spId, String spSecret, String secretName) {
    KeyVaultClient kvClient = createKeyVaultClient(spId, spSecret);
    return kvClient.getSecret(this.keyVaultUrl, secretName);
  }

  /**
   * Retrieve a secret of the name {@code secretName} as the service principal identified using {@code spId} and {@code secretName}
   *
   * @param spId          the id of service principal who is able to access the key vault
   * @param spSecret      the secret of service principal who is able to access the key vault
   * @param secretName    the name of the secret you want to retrieve
   * @param secretVersion the version of the secret you want to retrieve
   * @return
   */
  public SecretBundle getSecret(String spId, String spSecret, String secretName, String secretVersion) {
    KeyVaultClient kvClient = createKeyVaultClient(spId, spSecret);
    return kvClient.getSecret(this.keyVaultUrl, secretName, secretVersion);
  }

  /**
   * @param spId     the service principal id
   * @param spSecret the service principal secret
   * @return the key vault client for a service principal
   */
  private static KeyVaultClient createKeyVaultClient(String spId, String spSecret) {
    ServiceClientCredentials credentials = createCredentials(spId, spSecret);
    return new KeyVaultClient(credentials);
  }

  /**
   * Fetch the authentication token against Azure Active Directory in order to access the Azure Key Vault
   *
   * @param spId     the service principal id
   * @param spSecret the service principal secret
   * @return a new key vault ServiceClientCredentials based on ADAL authentication for this service principal
   */
  private static ServiceClientCredentials createCredentials(String spId, String spSecret) {
    return new KeyVaultCredentials() {
      /**
       * Callback that supplies and access token on request.
       */
      @Override
      public String doAuthenticate(String authorization, String resource, String scope) {
        log.debug("Authorization URL: " + authorization);
        /**
         * the resource is the same as {@link org.apache.gobblin.azure.aad.AADTokenRequesterImpl#TOKEN_TARGET_RESOURCE_KEY_VAULT}
         */
        log.debug("Resource: " + resource); //value is:   https://vault.azure.net
        log.debug("Scope: " + scope);
        log.debug("Performing OAuth Authentication for Service Principal: " + spId);

        try {
          CachedAADAuthenticator cachedAADAuthenticator = new CachedAADAuthenticator(authorization);
          AuthenticationResult token = cachedAADAuthenticator.getToken(resource, spId, spSecret);
          return token.getAccessToken();
        } catch (Exception e) {
          log.error("Failed to get AAD authentication token");
          throw new RuntimeException(e);
        }
      }
    };
  }
}