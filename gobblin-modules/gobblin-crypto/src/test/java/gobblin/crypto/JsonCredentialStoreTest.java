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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class JsonCredentialStoreTest {
  private static final byte[] KEY1_EXPECTED_VAL = new byte[] { 2, 79, 74, 11, 93, -118, 15, 29, -97, -78, 64, -7, -89, 74, 63, -119 };

  @DataProvider(name="codecInfo")
  public static Object[][] codecInfo() {
    return new Object[][] {
        { HexKeyToStringCodec.TAG, new HexKeyToStringCodec() },
        { Base64KeyToStringCodec.TAG, new Base64KeyToStringCodec() }
    };
  }

  @Test(dataProvider = "codecInfo")
  public void canLoadKeystore(String codecType, KeyToStringCodec codec) throws IOException {
    Path ksPath = new Path(getClass().getResource("/crypto/test_json_keystore." + codecType + ".json").toString());
    JsonCredentialStore credentialStore = new JsonCredentialStore(ksPath, codec);

    Map<String, byte[]> allKeys = credentialStore.getAllEncodedKeys();
    Assert.assertEquals(allKeys.size(), 29);
    Assert.assertEquals(credentialStore.getEncodedKey("0001"), KEY1_EXPECTED_VAL);

    for (Map.Entry<String, byte[]> key : allKeys.entrySet()) {
      Assert.assertEquals(credentialStore.getEncodedKey(key.getKey()), key.getValue());
      Assert.assertEquals(key.getValue().length, 16);
    }
  }
}
