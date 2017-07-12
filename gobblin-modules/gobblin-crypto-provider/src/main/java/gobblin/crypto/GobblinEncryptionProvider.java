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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import gobblin.annotation.Alpha;
import gobblin.codec.StreamCodec;

import lombok.extern.slf4j.Slf4j;


/**
 * Helper and factory methods for encryption algorithms.
 *
 * Note: Interface will likely change to support registration of algorithms
 */
@Slf4j
@Alpha
public class GobblinEncryptionProvider implements CredentialStoreProvider, EncryptionProvider {
  private final static Set<String> SUPPORTED_STREAMING_ALGORITHMS =
      ImmutableSet.of("aes_rotating", EncryptionConfigParser.ENCRYPTION_TYPE_ANY);

  /**
   * Return a set of streaming algorithms (StreamEncoders) that this factory knows how to build
   * @return Set of streaming algorithms the factory knows how to build
   */
  public static Set<String> supportedStreamingAlgorithms() {
    return SUPPORTED_STREAMING_ALGORITHMS;
  }

  /**
   * Return a StreamEncryptor for the given parameters. The algorithm type to use will be extracted
   * from the parameters object.
   * @param parameters Configured parameters for algorithm.
   * @return A StreamCodec for the requested algorithm
   * @throws IllegalArgumentException If the given algorithm/parameter pair cannot be built
   */
  public StreamCodec buildStreamEncryptor(Map<String, Object> parameters) {
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
   * @return A StreamEncoder for that algorithm
   * @throws IllegalArgumentException If the given algorithm/parameter pair cannot be built
   */
  public StreamCodec buildStreamCryptoProvider(String algorithm, Map<String, Object> parameters) {
    switch (algorithm) {
      case EncryptionConfigParser.ENCRYPTION_TYPE_ANY:
      case "aes_rotating":
        CredentialStore cs = CredentialStoreFactory.buildCredentialStore(parameters);
        if (cs == null) {
          throw new IllegalArgumentException("Failed to build credential store; can't instantiate AES");
        }

        return new RotatingAESCodec(cs);
      case GPGCodec.TAG:
        String password = EncryptionConfigParser.getKeystorePassword(parameters);
        Preconditions.checkNotNull(password, "Must specify an en/decryption password for GPGCodec!");
        return new GPGCodec(password);

      default:
        log.debug("Do not support encryption type {}", algorithm);
        return null;
    }
  }

  /**
   * Build a credential store with the given parameters.
   */
  public CredentialStore buildCredentialStore(Map<String, Object> parameters) {
    String ks_type = EncryptionConfigParser.getKeystoreType(parameters);
    String ks_path = EncryptionConfigParser.getKeystorePath(parameters);
    String ks_password = EncryptionConfigParser.getKeystorePassword(parameters);

    try {
      switch (ks_type) {
        // TODO this is yet another example of building a broad type (CredentialStore) based on a human-readable name
        // (json) with a bag of parameters. Need to pull out into its own pattern!
        case JCEKSKeystoreCredentialStore.TAG:
          return new JCEKSKeystoreCredentialStore(ks_path, ks_password);
        case JsonCredentialStore.TAG:
          return new JsonCredentialStore(ks_path, buildKeyToStringCodec(parameters));
        default:
          return null;
      }
    } catch (IOException e) {
      log.error("Error building credential store, returning null", e);
      return null;
    }
  }

  /**
   * Build a KeyToStringCodec based on parameters. To reduce complexity we don't build these
   * through a ServiceLocator since hopefully the # of key encodings is small.
   * @param parameters Config parameters used to build the codec
   */
  private KeyToStringCodec buildKeyToStringCodec(Map<String, Object> parameters) {
    String encodingName = EncryptionConfigParser.getKeystoreEncoding(parameters);
    switch (encodingName) {
      case HexKeyToStringCodec.TAG:
        return new HexKeyToStringCodec();
      case Base64KeyToStringCodec.TAG:
        return new Base64KeyToStringCodec();
      default:
        throw new IllegalArgumentException("Don't know how to build key to string codec for type " + encodingName);
    }
  }

  public GobblinEncryptionProvider() {
    // for ServiceLocator
  }
}
