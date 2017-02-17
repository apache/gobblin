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

package gobblin.converter;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.type.RecordWithMetadata;
import gobblin.type.SerializedRecord;
import gobblin.type.SerializedRecordWithMetadata;


public class AnyToSerializedRecordConverterTest {

  private static final Gson GSON = SerializedRecord.getSerializedAwareGson();
  private AnyToSerializedRecordConverter converter;

  @BeforeTest
  public void initConverter() {
    converter = new AnyToSerializedRecordConverter();
  }

  @Test
  public void testSerializedRecordWithMetadata()
      throws DataConversionException, UnsupportedEncodingException {
    final String origText = "hello there";
    final List<String> origContentTypes = ImmutableList.of("text/plain", "text/foobar");
    SerializedRecord r = new SerializedRecord(ByteBuffer.wrap(origText.getBytes("UTF-8")), origContentTypes);

    Map<String, Object> metadata = ImmutableMap.<String, Object>of("foo", "bar", "boo", "baz");

    Iterable<SerializedRecord> output =
        converter.convertRecord("", new SerializedRecordWithMetadata(r, metadata), null);

    SerializedRecord serializedRecord = output.iterator().next();

    /* Verify the outer record -- should look something like:
     * {
     *   contentTypes: ['recordWMetadata+json']
     *   record: "{\"contentTypes\": [\"text/plain\"], \"record\":...}", \"metadata\":{\"foo\":\"bar\"}
     *  }
     */
    JsonObject parsedJson = GSON.fromJson(serializedRecord.toJsonString(), JsonObject.class);
    JsonArray outerContentType = parsedJson.getAsJsonArray("contentTypes");
    Assert.assertEquals(outerContentType.size(), SerializedRecordWithMetadata.CONTENT_TYPE_JSON.size());
    for (int i = 0; i < outerContentType.size(); i++) {
      Assert.assertEquals(outerContentType.get(i).getAsString(), SerializedRecordWithMetadata.CONTENT_TYPE_JSON.get(i));
    }

    /*
     * Now verify the inner record can be parsed properly
     *
     */
    String wrappedJsonRecordString = parsedJson.get("record").getAsString();
    JsonObject innerBody = GSON.fromJson(wrappedJsonRecordString, JsonObject.class);

    /*
     * 'record' contains a SerializedRecord
     */
    SerializedRecord innerRecord = GSON.fromJson(innerBody.getAsJsonObject("record"), SerializedRecord.class);
    Assert.assertEquals(innerRecord.getContentTypes(), origContentTypes);

    ByteBuffer parsedBuf = innerRecord.getRecord();
    byte[] parsedBytes = new byte[parsedBuf.remaining()];
    parsedBuf.get(parsedBytes);

    String parsedText = new String(parsedBytes, "UTF-8");
    Assert.assertEquals(parsedText, origText);

    /*
     * 'metadata' contains the metadata associated with it
     */
    JsonObject parsedMetadata = innerBody.getAsJsonObject("metadata");
    for (Map.Entry<String, Object> entry : metadata.entrySet()) {
      Assert.assertEquals(parsedMetadata.get(entry.getKey()).getAsString(), entry.getValue());
    }
  }

  @Test
  public void testWithByteArray()
      throws DataConversionException {
    byte[] input = new byte[]{'a', 'b', 'c', 'd'};
    Iterable<SerializedRecord> it = converter.convertRecord(null, input, null);
    SerializedRecord serRecord = it.iterator().next();

    Assert.assertEquals(serRecord.getRecord().array(), input);
    Assert.assertEquals(1, serRecord.getContentTypes().size());
    Assert.assertEquals(serRecord.getContentTypes().get(0), "application/octet-stream");
  }

  @Test
  public void testWithJsonObject()
      throws DataConversionException, UnsupportedEncodingException {
    Gson gson = new Gson();
    SimpleObject simpleObject = new SimpleObject("abc", 2);

    JsonElement json = gson.toJsonTree(simpleObject);

    Iterable<SerializedRecord> it = converter.convertRecord(null, json, null);
    SerializedRecord serRecord = it.iterator().next();

    Assert.assertEquals(serRecord.getRecord().array(), gson.toJson(json).getBytes("UTF-8"));
    Assert.assertEquals(1, serRecord.getContentTypes().size());
    Assert.assertEquals(serRecord.getContentTypes().get(0), "application/json");
  }

  @Test
  public void testJsonObjectWithMetadata()
      throws DataConversionException, UnsupportedEncodingException {
    final String origContentType = "application/vnd.simpleobject+json";
    final Map<String, Object> origMetadata = ImmutableMap.<String, Object>of(
        RecordWithMetadata.RECORD_NAME, origContentType,
        "foo", "bar"
    );

    Gson gson = new Gson();
    SimpleObject simpleObject = new SimpleObject("abc", 2);
    JsonElement json = gson.toJsonTree(simpleObject);
    RecordWithMetadata<JsonElement> jsonElementRecordWithMetadata =
        new RecordWithMetadata<>(json, origMetadata);

    Iterable<SerializedRecord> it = converter.convertRecord(null, jsonElementRecordWithMetadata, null);
    SerializedRecord outerSerRecord = it.iterator().next();
    Assert.assertEquals(outerSerRecord.getContentTypes(), SerializedRecordWithMetadata.CONTENT_TYPE_JSON);

    SerializedRecordWithMetadata innerRecord =
        GSON.fromJson(new String(outerSerRecord.getRecord().array(), "UTF-8"), SerializedRecordWithMetadata.class);

    // Verify the serializedRecord decodes back to SimpleObject
    Assert.assertEquals(innerRecord.getRecord().getRecord().array(), gson.toJson(json).getBytes("UTF-8"));
    Assert.assertEquals(1, innerRecord.getRecord().getContentTypes().size());
    Assert.assertEquals(innerRecord.getRecord().getContentTypes().get(0), origContentType);

    // Verify metadata is still present
    for (Map.Entry<String, Object> entry: origMetadata.entrySet()) {
      Assert.assertEquals(innerRecord.getMetadata().get(entry.getKey()), entry.getValue());
    }
  }

  static class SimpleObject {
    private final String str;
    private final long l;

    public SimpleObject(String str, long l) {
      this.str = str;
      this.l = l;
    }

    public String getStr() {
      return str;
    }

    public long getL() {
      return l;
    }
  }
}
