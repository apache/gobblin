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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import gobblin.crypto.Base64Codec;
import gobblin.util.io.StreamUtils;


/**
 * Represents a record that has been serialized to a byte-stream through a set of encodings. A string
 * of content-types are also tracked with the record, which represent the original content-type + any encodings
 * that the record has gone through. Eg a jpeg file that has since been base64 encoded would have
 * content-types: ['image/jpeg', 'base64'].
 */
@Getter
@AllArgsConstructor
public class SerializedRecord {
  private final ByteBuffer record;
  private final List<String> contentTypes;

  /**
   * Convert this record to JSON format. If the content-types of the record indicate the record is not
   * UTF-8 printable, this function will automatically base64 encode the object, add 'base64' to the
   * content-types array, and set the 'record' field to the base64 encoded version.
   *
   * @return JSON representation of the record
   */
  public String toJsonString() {
    return GSON.toJson(this);
  }

  final private static Gson GSON;

  /**
   * Retrieve a Gson object that understands how to serialize SerializedRecords
   */
  public static Gson getSerializedAwareGson() {
    return GSON;
  }

  static {
    GsonBuilder builder = new GsonBuilder();
    builder.disableHtmlEscaping().registerTypeAdapter(SerializedRecord.class, new SerializedRecordGsonSerDe());
    GSON = builder.create();
  }

  private boolean areContentsUtf8Encoded() {
    // We care about the _last_ encoding applied to the object, so pull from the end of the content-types
    // array
    String lastContentType = (contentTypes.isEmpty()) ? "" : contentTypes.get(contentTypes.size() - 1);

    return ContentTypeUtils.getInstance().getCharset(lastContentType).equals("UTF-8");
  }

  private static class SerializedRecordGsonSerDe implements JsonSerializer<SerializedRecord>, JsonDeserializer<SerializedRecord> {
    @Override
    public JsonElement serialize(SerializedRecord src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject ret = new JsonObject();
      JsonArray contentTypes = new JsonArray();
      JsonPrimitive record;

      for (String contentType : src.getContentTypes()) {
        contentTypes.add(contentType);
      }

      if (src.areContentsUtf8Encoded()) {
        record = new JsonPrimitive(interpretBufferAsUtf8String(src.getRecord()));
      } else {
        record = new JsonPrimitive(encodeBufferAsBase64(src.getRecord()));
        contentTypes.add("base64");
      }

      ret.add("contentTypes", contentTypes);
      ret.add("record", record);

      return ret;
    }

    @Override
    public SerializedRecord deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject root = json.getAsJsonObject();

      List<String> contentTypes = context.deserialize(root.getAsJsonArray("contentTypes"), List.class);
      ByteBuffer record = ByteBuffer.wrap(root.get("record").getAsString().getBytes(Charset.forName("UTF-8")));

      return new SerializedRecord(record, contentTypes);
    }

    private String encodeBufferAsBase64(ByteBuffer record) {
      try {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        OutputStream base64Stream = new Base64Codec().encodeOutputStream(sink);

        StreamUtils.byteBufferToOutputStream(record, base64Stream);

        base64Stream.close();

        return sink.toString("UTF-8");
      } catch (IOException e) {
        throw new AssertionError("Should never get IOException writing to bytearrayoutputstreams");
      }
    }

    private String interpretBufferAsUtf8String(ByteBuffer record) {
      if (record.hasArray()) {
        return new String(record.array(), record.arrayOffset() + record.position(), record.remaining(),
            Charset.forName("UTF-8"));
      } else {
        final byte[] b = new byte[record.remaining()];
        record.duplicate().get(b);
        return new String(b, Charset.forName("UTF-8"));
      }
    }
  }
}
