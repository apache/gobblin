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
package gobblin.metadata.types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents metadata for a pipeline. There are two 'levels' of metadata - one that is global to an entire
 * dataset, and one that is applicable to each file present in a dataset.
 */
public class GlobalMetadata {
  private static final Logger log = LoggerFactory.getLogger(GlobalMetadata.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final JsonFactory jsonFactory = new JsonFactory();

  @JsonProperty("dataset")
  private final Map<String, Object> datasetLevel;

  @JsonProperty("file")
  private final Map<String, Map<String, Object>> fileLevel;

  private final static String DATASET_URN_KEY = "Dataset-URN";
  private final static String TRANSFER_ENCODING_KEY = "Transfer-Encoding";

  /**
   * Create a new, empty, metadata descriptor.
   */
  public GlobalMetadata() {
    datasetLevel = new ConcurrentHashMap<>();
    fileLevel = new ConcurrentHashMap<>();
  }

  /**
   * Create a new GlobalMetadata object from its serialized representation.
   * @throws IOException If the JSON string cannot be parsed.
   */
  public static GlobalMetadata fromJson(String json)
      throws IOException {
    return objectMapper.readValue(json, GlobalMetadata.class);
  }

  /**
   * Merge another GlobalMetadata object into this one. All keys from 'other' will be placed into
   * this object, replacing any already existing keys.
   * @param other Metadata object to add
   */
  public void addAll(GlobalMetadata other) {
    datasetLevel.putAll(other.datasetLevel);
    for (Map.Entry<String, Map<String, Object>> e : other.fileLevel.entrySet()) {
      Map<String, Object> val = new ConcurrentHashMap<>();
      val.putAll(e.getValue());
      fileLevel.put(e.getKey(), val);
    }
  }

  /**
   * Merge default settings into this object. Logic is very similar to addAll(), but Transfer-Encoding gets
   * special treatment; the 'default' transfer-encoding settings are appended to any transfer-encoding
   * already set (vs simply overwriting them).
   */
  public static GlobalMetadata metadataMergedWithDefaults(GlobalMetadata orig, GlobalMetadata defaults) {
    GlobalMetadata newMd = new GlobalMetadata();
    newMd.addAll(orig);

    List<String> defaultTransferEncoding = defaults.getTransferEncoding();
    List<String> myEncoding = orig.getTransferEncoding();

    if (defaultTransferEncoding != null) {
      if (myEncoding == null) {
        newMd.setDatasetMetadata(TRANSFER_ENCODING_KEY, defaultTransferEncoding);
      } else {
        List<String> combinedEncoding = new ArrayList<>();
        combinedEncoding.addAll(myEncoding);
        combinedEncoding.addAll(defaultTransferEncoding);

        newMd.setDatasetMetadata(TRANSFER_ENCODING_KEY, combinedEncoding);
      }
    }

    for (Map.Entry<String, Object> entry : defaults.datasetLevel.entrySet()) {
      if (!newMd.datasetLevel.containsKey(entry.getKey())) {
        newMd.datasetLevel.put(entry.getKey(), entry.getValue());
      }
    }

    return newMd;
  }

  /**
   * Serialize as a UTF8 encoded JSON string.
   */
  public byte[] toJsonUtf8() {
    try {
      ByteArrayOutputStream bOs = new ByteArrayOutputStream(512);

      try (JsonGenerator generator = jsonFactory.createJsonGenerator(bOs, JsonEncoding.UTF8)
          .setCodec(objectMapper)) {
        toJsonUtf8(generator);
      }

      return bOs.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Unexpected IOException serializing to ByteArray", e);
    }
  }

  /**
   * Serialize as a String
   */
  public String toJson()
      throws IOException {
    StringWriter writer = new StringWriter();
    try (JsonGenerator generator = jsonFactory.createJsonGenerator(writer)
        .setCodec(objectMapper)) {
      toJsonUtf8(generator);
    }

    return writer.toString();
  }

  /**
   * Write this object out to an existing JSON stream
   */
  protected void toJsonUtf8(JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();

    generator.writeObjectField("dataset", datasetLevel);
    generator.writeObjectFieldStart("file");
    for (Map.Entry<String, Map<String, Object>> entry : fileLevel.entrySet()) {
      generator.writeObjectField(entry.getKey(), entry.getValue());
    }
    generator.writeEndObject();
    generator.writeEndObject();
    generator.flush();
  }

  // Dataset-level metadata

  /**
   * Convenience method to retrieve the Dataset-URN dataset-level property.
   */
  public String getDatasetUrn() {
    return (String) datasetLevel.get(DATASET_URN_KEY);
  }

  /**
   * Convenience method to set the Dataset-URN property.
   */
  public void setDatasetUrn(String urn) {
    setDatasetMetadata(DATASET_URN_KEY, urn);
  }

  /**
   * Get an arbitrary dataset-level metadata key
   */
  public Object getDatasetMetadata(String key) {
    return datasetLevel.get(key);
  }

  /**
   * Set an arbitrary dataset-level metadata key
   */
  public void setDatasetMetadata(String key, Object val) {
    datasetLevel.put(key, val);
  }

  /**
   * Convenience method to retrieve the transfer-encodings that have been applied to the dataset
   */
  @SuppressWarnings("unchecked")
  public List<String> getTransferEncoding() {
    return (List<String>) getDatasetMetadata(TRANSFER_ENCODING_KEY);
  }

  /**
   * Convenience method to add a new transfer-encoding to a dataset
   */
  public synchronized void addTransferEncoding(String encoding) {
    List<String> encodings = getTransferEncoding();
    if (encodings == null) {
      encodings = new ArrayList<>();
    }

    encodings.add(encoding);

    setDatasetMetadata(TRANSFER_ENCODING_KEY, encodings);
  }

  // File-level  metadata

  /**
   * Get an arbitrary file-level metadata key
   */
  public Object getFileMetadata(String file, String key) {
    Map<String, Object> fileKeys = fileLevel.get(file);
    if (fileKeys == null) {
      return null;
    }

    return fileKeys.get(key);
  }

  /**
   * Set an arbitrary file-level metadata key
   */
  public void setFileMetadata(String file, String key, Object val) {
    Map<String, Object> fileKeys = fileLevel.get(file);
    if (fileKeys == null) {
      fileKeys = new ConcurrentHashMap<>();
      fileLevel.put(file, fileKeys);
    }

    fileKeys.put(key, val);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GlobalMetadata that = (GlobalMetadata) o;

    if (!datasetLevel.equals(that.datasetLevel)) {
      return false;
    }
    return fileLevel.equals(that.fileLevel);
  }

  @Override
  public int hashCode() {
    int result = datasetLevel.hashCode();
    result = 31 * result + fileLevel.hashCode();
    return result;
  }
}
