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
package gobblin.test;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import gobblin.crypto.CredentialStore;
import gobblin.test.crypto.TestEncryptionProvider;
import gobblin.test.crypto.TestRandomCredentialStore;


public class RandomCredentialStoreTest {
  @Test
  public void testSuccess() {
    Map<String, Object> params =
        ImmutableMap.<String, Object>of("keystore_type", TestRandomCredentialStore.TAG, "num_keys", "4");

    CredentialStore store = new TestEncryptionProvider().buildCredentialStore(params);

    Assert.assertNotNull(store);
    Assert.assertEquals(store.getAllEncodedKeys().size(), 4);
  }

  @Test
  public void testSeedsGiveRepeatableKeys() {
    Map<String, Object> params =
        ImmutableMap.<String, Object>of("keystore_type", TestRandomCredentialStore.TAG, "num_keys", "1", "random_seed",
            "12345");

    byte[] expectedKey = new byte[]{-42, 32, -97, 92, 49, -77, 97, -125, 34, -87, -40, -18, 120, 7, -56, -22};

    CredentialStore store = new TestEncryptionProvider().buildCredentialStore(params);

    Assert.assertNotNull(store);
    Assert.assertEquals(store.getAllEncodedKeys().size(), 1);
    Assert.assertEquals(store.getEncodedKey("0"), expectedKey);
  }
}
