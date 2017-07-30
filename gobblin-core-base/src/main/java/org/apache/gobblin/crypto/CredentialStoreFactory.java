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
package gobblin.crypto;

import java.util.Map;
import java.util.ServiceLoader;

import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import gobblin.codec.StreamCodec;


/**
 * This class knows how to build encryption algorithms based on configuration parameters. To add your own
 * encryption implementation, please add another implementation of {@link EncryptionProvider}
 * in Gobblin's classpath as well as referencing the new implementation in META-INF/services/gobblin.crypto.EncryptionProvider
 * of the containing JAR. (See {@link ServiceLoader} documentation for more details).
 */
@Slf4j
public class CredentialStoreFactory {
  private static ServiceLoader<CredentialStoreProvider> credentialStoreProviderLoader = ServiceLoader.load(CredentialStoreProvider.class);

  /**
   * Build a CredentialStore with the given config parameters. The type will be extracted from the parameters.
   * See {@link EncryptionConfigParser} for a set of standard configuration parameters, although
   * each encryption provider may have its own arbitrary set.
   * @return A CredentialStore for the given parameters
   * @throws IllegalArgumentException If no provider exists that can build the requested encryption codec
   */
  @Synchronized
  public static CredentialStore buildCredentialStore(Map<String, Object> parameters) {
    String credType = EncryptionConfigParser.getKeystoreType(parameters);

    for (CredentialStoreProvider provider : credentialStoreProviderLoader) {
      log.debug("Looking for cred store type {} in provider {}", credType, provider.getClass().getName());
      CredentialStore credStore = provider.buildCredentialStore(parameters);
      if (credStore != null) {
        log.debug("Found cred store type {} in provider {}", credType, provider.getClass().getName());
        return credStore;
      }
    }

    throw new IllegalArgumentException("Could not find a provider to build algorithm " + credType + " - is gobblin-crypto-provider in classpath?");
  }
}
