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

import com.google.common.collect.ImmutableSet;

import gobblin.writer.StreamCodec;

import lombok.extern.slf4j.Slf4j;


/**
 * Helper and factory methods for encryption algorithms.
 */
@Slf4j
public class EncryptionFactory {
  private final static Set<String> SUPPORTED_STREAMING_ALGORITHMS =
      ImmutableSet.of("insecure_shift", "aes_rotating", EncryptionConfigParser.ENCRYPTION_TYPE_ANY);

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
  public static StreamCodec buildStreamEncryptor(Map<String, Object> parameters) {
    String encryptionType = EncryptionConfigParser.getEncryptionType(parameters);
    if (encryptionType == null) {
      throw new IllegalArgumentException("Encryption type not present in parameters!");
    }

    return buildStreamEncryptor(encryptionType, parameters);
  }

  /**
   * Return a StreamEncryptor for the given algorithm and with appropriate parameters.
   * @param algorithm ALgorithm to build
   * @param parameters Parameters for algorithm
   * @return A SreamEncoder for that algorithm
   * @throws IllegalArgumentException If the given algorithm/parameter pair cannot be built
   */
  public static StreamCodec buildStreamEncryptor(String algorithm, Map<String, Object> parameters) {
    /* TODO - Ideally this would dynamically discover plugins somehow which would let us move
     * move crypto algorithms into gobblin-modules and just keep the factory in core. (The factory
     * would fail to build anything if the corresponding gobblin-modules aren't included).
     */
    switch (algorithm) {
      case "insecure_shift":
        return new InsecureShiftCodec(parameters);
      case EncryptionConfigParser.ENCRYPTION_TYPE_ANY:
      case "aes_rotating":
        CredentialStore cs = buildCredentialStore(parameters);
        if (cs == null) {
          throw new IllegalArgumentException("Failed to build credential store; can't instantiate AES");
        }

        return new RotatingAESCodec(cs);
      default:
        throw new IllegalArgumentException("Do not support encryption type " + algorithm);
    }
  }

  private static CredentialStore buildCredentialStore(Map<String, Object> parameters) {
    String ks_path = EncryptionConfigParser.getKeystorePath(parameters);
    String ks_password = EncryptionConfigParser.getKeystorePassword(parameters);

    try {
      return new JCEKSKeystoreCredentialStore(ks_path, ks_password);
    } catch (IOException e) {
      log.error("Error building credential store, returning null", e);
      return null;
    }
  }

  private EncryptionFactory() {
    // helper functions only, can't instantiate
  }
}
