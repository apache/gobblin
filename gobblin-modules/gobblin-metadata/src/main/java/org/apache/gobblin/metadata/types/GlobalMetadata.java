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
package org.apache.gobblin.metadata.types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

import javax.xml.bind.DatatypeConverter;


/**
 * Represents metadata for a pipeline. There are two 'levels' of metadata - one that is global to an entire
 * dataset, and one that is applicable to each file present in a dataset.
 */
public class GlobalMetadata {
  private static final Logger log = LoggerFactory.getLogger(GlobalMetadata.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final JsonFactory jsonFactory = new JsonFactory();
  private static final String EMPTY_ID = "0";

  @JsonProperty("dataset")
  private final Map<String, Object> datasetLevel;

  @JsonProperty("file")
  private final Map<String, Map<String, Object>> fileLevel;

  @JsonProperty("id")
  private String cachedId;

  private transient boolean markedImmutable;

  public final static String DATASET_URN_KEY = "Dataset-URN";
  public final static String TRANSFER_ENCODING_KEY = "Transfer-Encoding";
  public final static String CONTENT_TYPE_KEY = "Content-Type";
  public final static String INNER_CONTENT_TYPE_KEY = "Inner-Content-Type";
  public final static String NUM_RECORDS_KEY = "Num-Records";
  public final static String NUM_FILES_KEY = "Num-Files";


  /**
   * Create a new, empty, metadata descriptor.
   */
  public GlobalMetadata() {
    this.datasetLevel = new ConcurrentHashMap<>();
    this.fileLevel = new ConcurrentHashMap<>();
    this.markedImmutable = false;
  }

  /**
   * Mark the metadata as immutable. Once this flag is set all attempts to modify the object
   * will fail with {@link UnsupportedOperationException}.
   */
  public void markImmutable() {
    this.markedImmutable = true;
  }

  public boolean isImmutable() {
    return this.markedImmutable;
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
    throwIfImmutable();

    datasetLevel.putAll(other.datasetLevel);
    for (Map.Entry<String, Map<String, Object>> e : other.fileLevel.entrySet()) {
      Map<String, Object> val = new ConcurrentHashMap<>();
      val.putAll(e.getValue());
      fileLevel.put(e.getKey(), val);
    }

    cachedId = null;
  }

  /**
   * Merge default settings into this object. Logic is very similar to addAll(), but Transfer-Encoding gets
   * special treatment; the 'default' transfer-encoding settings are appended to any transfer-encoding
   * already set (vs simply overwriting them).
   */
  public void mergeWithDefaults(GlobalMetadata defaults) {
    List<String> defaultTransferEncoding = defaults.getTransferEncoding();
    List<String> myEncoding = getTransferEncoding();

    if (defaultTransferEncoding != null) {
      if (myEncoding == null) {
        setDatasetMetadata(TRANSFER_ENCODING_KEY, defaultTransferEncoding);
      } else {
        List<String> combinedEncoding = new ArrayList<>();
        combinedEncoding.addAll(myEncoding);
        combinedEncoding.addAll(defaultTransferEncoding);

        setDatasetMetadata(TRANSFER_ENCODING_KEY, combinedEncoding);
      }
    }

    for (Map.Entry<String, Object> entry : defaults.datasetLevel.entrySet()) {
      if (!datasetLevel.containsKey(entry.getKey())) {
        setDatasetMetadata(entry.getKey(), entry.getValue());
      }
    }
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

  protected void toJsonUtf8(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField("id", getId());
    bodyToJsonUtf8(generator);
    generator.writeEndObject();
    generator.flush();
  }
  /**
   * Write this object out to an existing JSON stream
   */
  protected void bodyToJsonUtf8(JsonGenerator generator)
      throws IOException {

    generator.writeObjectField("dataset", datasetLevel);
    generator.writeObjectFieldStart("file");
    for (Map.Entry<String, Map<String, Object>> entry : fileLevel.entrySet()) {
      generator.writeObjectField(entry.getKey(), entry.getValue());
    }
    generator.writeEndObject();

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
   * Convenience method to set the Content-Type property
   */
  public void setContentType(String contentType) {
    setDatasetMetadata(CONTENT_TYPE_KEY, contentType);
  }

  /**
   * Convenience method to retrieve the Content-Type property
   * @return
   */
  public String getContentType() {
    return (String)getDatasetMetadata(CONTENT_TYPE_KEY);
  }

  /**
   * Convenience method to set the Inner-Content-Type property
   */
  public void setInnerContentType(String innerContentType) {
    setDatasetMetadata(INNER_CONTENT_TYPE_KEY, innerContentType);
  }

  /**
   * Convenience method to retrieve the Inner-Content-Type property
   */
  public String getInnerContentType() {
    return (String)getDatasetMetadata(INNER_CONTENT_TYPE_KEY);
  }

  /**
   * Convenience method to set the number of files in the dataset
   */
  public void setNumOutputFiles(int numFiles) {
    setDatasetMetadata(NUM_FILES_KEY, numFiles);
  }

  /**
   * Convenience method to set the number of records in the dataset
   */
  public void setNumRecords(long numRecords) {
    setDatasetMetadata(NUM_RECORDS_KEY, numRecords);
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
    throwIfImmutable();
    datasetLevel.put(key, val);
    cachedId = null;
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
    throwIfImmutable();
    List<String> encodings = getTransferEncoding();
    if (encodings == null) {
      encodings = new ArrayList<>();
    }

    encodings.add(encoding);

    setDatasetMetadata(TRANSFER_ENCODING_KEY, encodings);
  }

  public long getNumRecords() {
    // When reading from JSON, Jackson could parse as an int so we need to use a more generic type
    Number numRecords = (Number)getDatasetMetadata(NUM_RECORDS_KEY);
    return (numRecords != null) ? numRecords.longValue() : 0L;
  }

  public int getNumFiles() {
    Integer numFiles = (Integer)getDatasetMetadata(NUM_FILES_KEY);
    return (numFiles != null)  ? numFiles : 0;
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
    throwIfImmutable();
    Map<String, Object> fileKeys = fileLevel.get(file);
    if (fileKeys == null) {
      fileKeys = new ConcurrentHashMap<>();
      fileLevel.put(file, fileKeys);
    }

    fileKeys.put(key, val);
    cachedId = null;
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
    return this.getId().equals(that.getId());
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  public String getId() {
    if (cachedId != null) {
      return cachedId;
    }

    if (datasetLevel.size() == 0 && fileLevel.size() == 0) {
      cachedId = EMPTY_ID;
      return cachedId;
    }

    try {
      // ID is calculated by serializing body to JSON and then taking that hash
      ByteArrayOutputStream bOs = new ByteArrayOutputStream(512);
      MessageDigest md5Digest = MessageDigest.getInstance("MD5");

      try (JsonGenerator generator = jsonFactory.createJsonGenerator(bOs, JsonEncoding.UTF8).setCodec(objectMapper)) {
        generator.writeStartObject();
        bodyToJsonUtf8(generator);
        generator.writeEndObject();
      }

      byte[] digestBytes = md5Digest.digest(bOs.toByteArray());
      cachedId = DatatypeConverter.printHexBinary(digestBytes);
      return cachedId;
    } catch (IOException|NoSuchAlgorithmException e) {
      throw new RuntimeException("Unexpected exception generating id", e);
    }
  }

  public boolean isEmpty() {
    return getId().equals(EMPTY_ID);
  }

  private void throwIfImmutable() {
    if (this.markedImmutable) {
      throw new UnsupportedOperationException("Metadata is marked as immutable -- cannot modify");
    }
  }
}
