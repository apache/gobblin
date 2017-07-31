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

import org.apache.gobblin.codec.StreamCodec;


/**
 * Represents a builder that can construct encryption providers given a type and various
 * configuration parameters.
 */
public interface EncryptionProvider {
  /**
   * Build a StreamEncryptor with the given configuration parameters.
   *
   * If the provider cannot satisfy the request it should return null.
   */
  StreamCodec buildStreamCryptoProvider(String algorithm, Map<String, Object> parameters);
}
