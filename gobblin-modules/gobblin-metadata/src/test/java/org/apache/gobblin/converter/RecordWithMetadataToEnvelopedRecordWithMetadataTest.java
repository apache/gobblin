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

package org.apache.gobblin.converter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;


@Test
public class RecordWithMetadataToEnvelopedRecordWithMetadataTest {

  @Test
  public void testSuccessWithJson()
      throws SchemaConversionException, DataConversionException, IOException {
    final String innerContentType = "randomJsonRecord";

    ObjectMapper objectMapper = new ObjectMapper();
    RecordWithMetadataToEnvelopedRecordWithMetadata converter = new RecordWithMetadataToEnvelopedRecordWithMetadata();
    converter.convertSchema("", null);

    // Build Test Record
    HashMap<String, String> map = new HashMap<>();
    map.put("test", "test");
    map.put("value", "value");

    JsonNode jsonElement = objectMapper.valueToTree(map);
    Metadata md = new Metadata();
    md.getGlobalMetadata().setDatasetUrn("my-dataset");
    md.getGlobalMetadata().setContentType(innerContentType);
    md.getRecordMetadata().put("foo", "bar");

    RecordWithMetadata<JsonNode> record = new RecordWithMetadata(jsonElement, md);
    // Convert it
    Iterator<RecordWithMetadata<byte[]>> recordWithMetadataIterator =
        converter.convertRecord("", record, null).iterator();
    RecordWithMetadata recordWithMetadata = recordWithMetadataIterator.next();

    // Verify it
    JsonNode parsedElement = objectMapper.readValue((byte[]) recordWithMetadata.getRecord(), JsonNode.class);
    Assert.assertEquals(parsedElement.get("mId").getTextValue(), record.getMetadata().getGlobalMetadata().getId());
    Assert.assertEquals(parsedElement.get("r"), jsonElement);
    Assert.assertEquals(parsedElement.get("rMd").get("foo").getTextValue(), "bar");

    Assert
        .assertEquals(recordWithMetadata.getMetadata().getGlobalMetadata().getContentType(), "lnkd+recordWithMetadata");
    Assert.assertEquals(recordWithMetadata.getMetadata().getGlobalMetadata().getInnerContentType(), innerContentType);
  }

  @Test
  public void testSuccessWithString()
      throws DataConversionException, IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    String innerRecord = "abracadabra";
    RecordWithMetadataToEnvelopedRecordWithMetadata converter = new RecordWithMetadataToEnvelopedRecordWithMetadata();
    RecordWithMetadata<String> record = new RecordWithMetadata<>(innerRecord, new Metadata());

    Iterator<RecordWithMetadata<byte[]>> recordWithMetadataIterator =
        converter.convertRecord("", record, null).iterator();
    RecordWithMetadata recordWithMetadata = recordWithMetadataIterator.next();

    JsonNode parsedElement = objectMapper.readValue((byte[]) recordWithMetadata.getRecord(), JsonNode.class);
    Assert.assertEquals(parsedElement.get("mId").getTextValue(), record.getMetadata().getGlobalMetadata().getId());
    Assert.assertEquals(parsedElement.get("r").getTextValue(), innerRecord);

    Assert
        .assertEquals(recordWithMetadata.getMetadata().getGlobalMetadata().getContentType(), "lnkd+recordWithMetadata");
    Assert.assertNull(recordWithMetadata.getMetadata().getGlobalMetadata().getInnerContentType());
  }

  @Test
  public void testSuccessWithInferredPrintableByteArray()
      throws DataConversionException, IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    byte[] record = "abrac\\adabra".getBytes(StandardCharsets.UTF_8);

    Metadata md = new Metadata();
    md.getGlobalMetadata().setContentType("application/binary");
    md.getGlobalMetadata().addTransferEncoding("base64");

    RecordWithMetadataToEnvelopedRecordWithMetadata converter = new RecordWithMetadataToEnvelopedRecordWithMetadata();

    Iterator<RecordWithMetadata<byte[]>> recordWithMetadataIterator =
        converter.convertRecord("", new RecordWithMetadata<>(record, md), null).iterator();
    RecordWithMetadata recordWithMetadata = recordWithMetadataIterator.next();

    JsonNode parsedElement = objectMapper.readValue((byte[]) recordWithMetadata.getRecord(), JsonNode.class);
    Assert.assertEquals(parsedElement.get("r").getTextValue(), new String(record, StandardCharsets.UTF_8));
  }

  @Test
  public void testSuccessWithBinary()
      throws DataConversionException, IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    byte[] record = "aaaa".getBytes(StandardCharsets.UTF_8);

    Metadata md = new Metadata();
    md.getGlobalMetadata().setContentType("application/binary");

    RecordWithMetadataToEnvelopedRecordWithMetadata converter = new RecordWithMetadataToEnvelopedRecordWithMetadata();

    Iterator<RecordWithMetadata<byte[]>> recordWithMetadataIterator =
        converter.convertRecord("", new RecordWithMetadata<>(record, md), null).iterator();
    RecordWithMetadata recordWithMetadata = recordWithMetadataIterator.next();

    JsonNode parsedElement = objectMapper.readValue((byte[]) recordWithMetadata.getRecord(), JsonNode.class);
    Assert.assertEquals(parsedElement.get("r").getTextValue(), "YWFhYQ==");
  }
}
