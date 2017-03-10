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

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.crypto.InsecureShiftCodec;
import gobblin.type.SerializedRecord;


public class SerializedRecordToEncryptedSerializedRecordConverterTest {
  private WorkUnitState workUnitState;
  private SerializedRecordToEncryptedSerializedRecordConverter converter;
  private SerializedRecord sampleRecord;
  private byte[] shiftedValue;
  private String insecureShiftTag;

  @BeforeTest
  public void setUp() {
    workUnitState = new WorkUnitState();
    converter = new SerializedRecordToEncryptedSerializedRecordConverter();
    sampleRecord = new SerializedRecord(ByteBuffer.wrap(new byte[]{'a', 'b', 'c', 'd'}),
        ImmutableList.of("application/octet-stream"));
    shiftedValue = new byte[]{'b', 'c', 'd', 'e'};
    insecureShiftTag = new InsecureShiftCodec(null).getTag();
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
        .setProp(EncryptionConfigParser.ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY + ".2",
            "insecure_shift");

    converter.init(workUnitState);
    Iterable<SerializedRecord> records = converter.convertRecord("", sampleRecord, workUnitState);
    Iterator<SerializedRecord> recordIt = records.iterator();
    Assert.assertTrue(recordIt.hasNext());

    SerializedRecord record = recordIt.next();

    Assert.assertFalse(recordIt.hasNext());
    Assert.assertEquals(record.getContentTypes().get(0), sampleRecord.getContentTypes().get(0));
    Assert.assertEquals(record.getContentTypes().get(1), insecureShiftTag);
    Assert.assertEquals(record.getRecord().array(), shiftedValue);
  }

  @Test
  public void worksNoFork()
      throws DataConversionException {
    workUnitState.getJobState()
        .setProp(EncryptionConfigParser.ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY,
            "insecure_shift");
    converter.init(workUnitState);
    Iterable<SerializedRecord> records = converter.convertRecord("", sampleRecord, workUnitState);
    Iterator<SerializedRecord> recordIt = records.iterator();
    Assert.assertTrue(recordIt.hasNext());

    SerializedRecord record = recordIt.next();

    Assert.assertFalse(recordIt.hasNext());
    Assert.assertEquals(record.getContentTypes().get(0), sampleRecord.getContentTypes().get(0));
    Assert.assertEquals(record.getContentTypes().get(1), insecureShiftTag);
    Assert.assertEquals(record.getRecord().array(), shiftedValue);
  }
}
