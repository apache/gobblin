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
import java.util.HashMap;
import java.util.Iterator;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class EnvelopedRecordWithMetadataToRecordWithMetadataTest {

  @Test
  public void testSuccessWithRecord() throws DataConversionException, IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    String innerRecord = "abracadabra";

    // Build the input record
    HashMap<String, Object> map = new HashMap<>();
    map.put("r", innerRecord);
    Metadata md = new Metadata();
    md.getRecordMetadata().put("test1", "test2");
    map.put("rMd", md);
    JsonNode jsonNode = objectMapper.valueToTree(map);
    RecordWithMetadata<byte[]> inputRecord = new RecordWithMetadata<>(jsonNode.toString().getBytes(), null);

    EnvelopedRecordWithMetadataToRecordWithMetadata converter = new EnvelopedRecordWithMetadataToRecordWithMetadata();
    Iterator<RecordWithMetadata<?>> iterator =
        converter.convertRecord(null, inputRecord, null).iterator();

    Assert.assertTrue(iterator.hasNext());

    RecordWithMetadata<?> outputRecord = iterator.next();

    Assert.assertEquals(outputRecord.getRecord(), innerRecord);
    Assert.assertEquals(outputRecord.getMetadata().getRecordMetadata().get("test1"), "test2");
  }

  @Test(expectedExceptions = DataConversionException.class, expectedExceptionsMessageRegExp = "Input data does not have record.")
  public void testFailureWithoutRecord() throws DataConversionException, IOException {
    ObjectMapper objectMapper = new ObjectMapper();

    // Build the input record without data
    HashMap<String, Object> map = new HashMap<>();
    Metadata md = new Metadata();
    md.getRecordMetadata().put("test1", "test2");
    map.put("rMd", md);
    JsonNode jsonNode = objectMapper.valueToTree(map);
    RecordWithMetadata<byte[]> inputRecord = new RecordWithMetadata<>(jsonNode.toString().getBytes(), null);

    EnvelopedRecordWithMetadataToRecordWithMetadata converter = new EnvelopedRecordWithMetadataToRecordWithMetadata();
    converter.convertRecord(null, inputRecord, null);
  }

}
