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
package gobblin.converter;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.type.RecordWithMetadata;
import gobblin.type.SerializedRecordWithMetadata;


public class RecordWithMetadataToEncryptedRecordWithMetadataConverterTest {
  private WorkUnitState workUnitState;
  private RecordWithMetadataToEncryptedRecordWithMetadataConverter converter;
  private RecordWithMetadata<byte[]> sampleRecord;
  private byte[] shiftedValue;

  @BeforeTest
  public void setUp() {
    workUnitState = new WorkUnitState();
    converter = new RecordWithMetadataToEncryptedRecordWithMetadataConverter();
    sampleRecord = new RecordWithMetadata<>(
        new byte[] { 'a', 'b', 'c', 'd'},
        ImmutableMap.<String, Object>of("key1", "value1", "key2", "value2"));
    shiftedValue = new byte[] { 'b', 'c', 'd', 'e'};
  }

  @Test(expectedExceptions = DataConversionException.class)
  public void throwsIfMisconfigured() throws DataConversionException {
    converter.convertRecord("", sampleRecord, workUnitState);
  }

  @Test
  public void worksWithFork() throws DataConversionException {
    workUnitState.setProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, 2);
    workUnitState.getJobState().setProp(EncryptionConfigParser.ENCRYPT_PREFIX + "." +
            EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY + ".2",
        "insecure_shift");

    Iterable<SerializedRecordWithMetadata> records = converter.convertRecord("", sampleRecord, workUnitState);
    Iterator<SerializedRecordWithMetadata> recordIt = records.iterator();
    Assert.assertTrue(recordIt.hasNext());

    SerializedRecordWithMetadata record = recordIt.next();

    Assert.assertFalse(recordIt.hasNext());
    Assert.assertEquals(record.getMetadata(), sampleRecord.getMetadata());
    Assert.assertEquals(record.getRecord(), shiftedValue);
  }

  @Test
  public void worksNoFork() throws DataConversionException {
    workUnitState.getJobState().setProp(EncryptionConfigParser.ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY,
        "insecure_shift");
    Iterable<SerializedRecordWithMetadata> records = converter.convertRecord("", sampleRecord, workUnitState);
    Iterator<SerializedRecordWithMetadata> recordIt = records.iterator();
    Assert.assertTrue(recordIt.hasNext());

    SerializedRecordWithMetadata record = recordIt.next();

    Assert.assertFalse(recordIt.hasNext());
    Assert.assertEquals(record.getMetadata(), sampleRecord.getMetadata());
    Assert.assertEquals(record.getRecord(), shiftedValue);
  }
}
