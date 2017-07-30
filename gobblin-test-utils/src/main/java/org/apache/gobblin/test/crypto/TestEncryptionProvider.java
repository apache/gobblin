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
package gobblin.test.crypto;

import java.util.Map;

import gobblin.codec.StreamCodec;
import gobblin.crypto.CredentialStore;
import gobblin.crypto.CredentialStoreProvider;
import gobblin.crypto.EncryptionProvider;


public class TestEncryptionProvider implements CredentialStoreProvider, EncryptionProvider {
  private static final String INSECURE_SHIFT_TAG = InsecureShiftCodec.TAG;

  @Override
  public CredentialStore buildCredentialStore(Map<String, Object> parameters) {
    String csType = (String)parameters.get("keystore_type"); // Don't want to take compile-time dependency on gobblin-core for this constant
    if (csType.equals(TestRandomCredentialStore.TAG)) {
      int numKeys = Integer.parseInt((String)parameters.getOrDefault("num_keys", "1"));

      String seedParam = (String)parameters.getOrDefault("random_seed", null);
      long seed = System.currentTimeMillis();
      if (seedParam != null) {
        seed = Long.parseLong(seedParam);
      }

      return new TestRandomCredentialStore(numKeys, seed);
    }

    return null;
  }

  @Override
  public StreamCodec buildStreamCryptoProvider(String algorithm, Map<String, Object> parameters) {
    switch (algorithm) {
      case INSECURE_SHIFT_TAG:
        return new InsecureShiftCodec(parameters);
      default:
        return null;
    }
  }
}
