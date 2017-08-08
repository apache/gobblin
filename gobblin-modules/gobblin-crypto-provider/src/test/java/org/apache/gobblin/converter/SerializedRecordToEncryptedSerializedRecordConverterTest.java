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

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.test.crypto.InsecureShiftCodec;
import org.apache.gobblin.type.RecordWithMetadata;


public class SerializedRecordToEncryptedSerializedRecordConverterTest {
  private WorkUnitState workUnitState;
  private SerializedRecordToEncryptedSerializedRecordConverter converter;
  private RecordWithMetadata<byte[]> sampleRecord;
  private byte[] shiftedValue;
  private String insecureShiftTag;

  private final String ENCRYPT_PREFIX = "converter.encrypt.";

  @BeforeTest
  public void setUp() {
    workUnitState = new WorkUnitState();
    converter = new SerializedRecordToEncryptedSerializedRecordConverter();
    sampleRecord = new RecordWithMetadata<>(new byte[]{'a', 'b', 'c', 'd'}, new Metadata());
    shiftedValue = new byte[]{'b', 'c', 'd', 'e'};
    insecureShiftTag = InsecureShiftCodec.TAG;
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void throwsIfMisconfigured()
      throws DataConversionException {
    converter.init(workUnitState);
    converter.convertRecord("", sampleRecord, workUnitState);
  }

  @Test
  public void worksWithFork()
      throws DataConversionException {
    workUnitState.setProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, 2);
    workUnitState.getJobState()
        .setProp(ENCRYPT_PREFIX + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY + ".2",
            "insecure_shift");

    converter.init(workUnitState);
    Iterable<RecordWithMetadata<byte[]>> records = converter.convertRecord("", sampleRecord, workUnitState);
    Iterator<RecordWithMetadata<byte[]>> recordIt = records.iterator();
    Assert.assertTrue(recordIt.hasNext());

    RecordWithMetadata<byte[]> record = recordIt.next();

    Assert.assertFalse(recordIt.hasNext());
    Assert.assertEquals(record.getMetadata().getGlobalMetadata().getTransferEncoding().get(0), insecureShiftTag);
    Assert.assertEquals(record.getRecord(), shiftedValue);
  }

  @Test
  public void worksNoFork()
      throws DataConversionException {
    workUnitState.getJobState()
        .setProp(ENCRYPT_PREFIX + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY,
            "insecure_shift");
    converter.init(workUnitState);
    Iterable<RecordWithMetadata<byte[]>> records = converter.convertRecord("", sampleRecord, workUnitState);
    Iterator<RecordWithMetadata<byte[]>> recordIt = records.iterator();
    Assert.assertTrue(recordIt.hasNext());

    RecordWithMetadata<byte[]> record = recordIt.next();

    Assert.assertFalse(recordIt.hasNext());
    Assert.assertEquals(record.getMetadata().getGlobalMetadata().getTransferEncoding().get(0), insecureShiftTag);
    Assert.assertEquals(record.getRecord(), shiftedValue);
  }
}
