/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package gobblin.type;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


public class SerializedRecordTest {
  private static Gson GSON;

  @BeforeClass
  public void initGson() {
    GSON = new Gson();
  }

  @Test
  public void testToJsonWithString() {
    String body = "hello world";
    ByteBuffer encodedBody = Charset.forName("UTF-8").encode(body);

    SerializedRecord r = new SerializedRecord(encodedBody, ImmutableList.of("text/plain"));
    String serialized = r.toJsonString();

    JsonObject decoded = GSON.fromJson(serialized, JsonObject.class);
    Assert.assertTrue(decoded.has("contentTypes"));
    JsonArray contentTypes = decoded.getAsJsonArray("contentTypes");
    Assert.assertEquals(1, contentTypes.size());
    Assert.assertEquals(contentTypes.get(0).getAsString(), "text/plain");

    String record = decoded.get("record").getAsString();
    Assert.assertEquals(record, body);
  }

  @Test
  public void testToJsonWithBinary() {
    String body = "abcd";
    ByteBuffer encodedBody = Charset.forName("UTF-8").encode(body);

    SerializedRecord r = new SerializedRecord(encodedBody, ImmutableList.of("application/octet-stream"));
    String serialized = r.toJsonString();

    // toJsonString() should have automatically base64 encoded the record since it doesn't have a printable
    // content type
    JsonObject decoded = GSON.fromJson(serialized, JsonObject.class);
    Assert.assertTrue(decoded.has("contentTypes"));
    JsonArray contentTypes = decoded.getAsJsonArray("contentTypes");
    Assert.assertEquals(2, contentTypes.size());
    Assert.assertEquals(contentTypes.get(0).getAsString(), "application/octet-stream");
    Assert.assertEquals(contentTypes.get(1).getAsString(), "base64");

    String record = decoded.get("record").getAsString();
    Assert.assertEquals(record, "YWJjZA==");
  }
}
