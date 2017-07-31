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
package org.apache.gobblin.crypto;

import java.util.Map;
import java.util.ServiceLoader;

import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.codec.StreamCodec;


/**
 * This class knows how to build encryption algorithms based on configuration parameters. To add your own
 * encryption implementation, please add another implementation of {@link org.apache.gobblin.crypto.EncryptionProvider}
 * in Gobblin's classpath as well as referencing the new implementation in META-INF/services/gobblin.crypto.EncryptionProvider
 * of the containing JAR. (See {@link java.util.ServiceLoader} documentation for more details).
 */
@Slf4j
public class EncryptionFactory {
  private static ServiceLoader<EncryptionProvider> encryptionProviderLoader = ServiceLoader.load(EncryptionProvider.class);

  /**
   * Build a StreamCodec with the given config parameters. The type will be extracted from the parameters.
   * See {@link org.apache.gobblin.crypto.EncryptionConfigParser} for a set of standard configuration parameters, although
   * each encryption provider may have its own arbitrary set.
   * @return A StreamCodec for the given parameters
   * @throws IllegalArgumentException If no provider exists that can build the requested encryption codec
   */
  public static StreamCodec buildStreamCryptoProvider(Map<String, Object> parameters) {
    String encryptionType = EncryptionConfigParser.getEncryptionType(parameters);
    if (encryptionType == null) {
      throw new IllegalArgumentException("Encryption type not present in parameters!");
    }

    return buildStreamCryptoProvider(encryptionType, parameters);
  }

  /**
   * Return a StreamEncryptor for the given algorithm and with appropriate parameters.
   * @param algorithm Algorithm to build
   * @param parameters Parameters for algorithm
   * @return A StreamCodec for that algorithm
   * @throws IllegalArgumentException If the given algorithm/parameter pair cannot be built
   */
  @Synchronized
  public static StreamCodec buildStreamCryptoProvider(String algorithm, Map<String, Object> parameters) {
    for (EncryptionProvider provider : encryptionProviderLoader) {
      log.debug("Looking for algorithm {} in provider {}", algorithm, provider.getClass().getName());
      StreamCodec codec = provider.buildStreamCryptoProvider(algorithm, parameters);
      if (codec != null) {
        log.debug("Found algorithm {} in provider {}", algorithm, provider.getClass().getName());
        return codec;
      }
    }

    throw new IllegalArgumentException("Could not find a provider to build algorithm " + algorithm + " - is gobblin-crypto-provider in classpath?");
  }
}
