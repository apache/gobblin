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


/**
 * Interface for a simple CredentialStore that simply has a set of byte-encoded keys. Format
 * of the underlying keys is left to the implementor.
 */
public interface CredentialStore {
  /**
   * Get the binary encoded key with the given key
   * @param id Key to lookup
   * @return null if the key does not exist; encoded-key if it does
   */
  byte[] getEncodedKey(String id);

  /**
   * List all binary encoded keys in the credential store
   * @return all binary encoded keys in the credential store
   */
  Map<String, byte[]> getAllEncodedKeys();
}
