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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JsonCredentialStoreTest {
  @Test
  public void canLoadKeystore() throws IOException {
    Path ksPath = new Path(getClass().getResource("/crypto/test_json_keystore.json").toString());
    JsonCredentialStore credentialStore = new JsonCredentialStore(ksPath);

    Map<String, byte[]> allKeys = credentialStore.getAllEncodedKeys();
    Assert.assertEquals(allKeys.size(), 29);
    for (Map.Entry<String, byte[]> key : allKeys.entrySet()) {
      Assert.assertEquals(credentialStore.getEncodedKey(key.getKey()), key.getValue());
      Assert.assertEquals(key.getValue().length, 16);
    }
  }

  @Test
  public void canBuildKeystore() {
    Path ksPath = new Path(getClass().getResource("/crypto/test_json_keystore.json").toString());
    String type = JsonCredentialStore.TAG;

    Map<String, Object> properties = new HashMap<>();
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, ksPath.toString());
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_TYPE_KEY, type);
    properties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "insecure_shift");

    Assert.assertNotNull(EncryptionFactory.buildStreamCryptoProvider(properties));
  }
}
