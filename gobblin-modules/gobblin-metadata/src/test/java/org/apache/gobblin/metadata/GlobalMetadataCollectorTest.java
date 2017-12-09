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
package org.apache.gobblin.metadata;

import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.metadata.types.GlobalMetadata;


public class GlobalMetadataCollectorTest {
  private static final String CONTENT_TYPE = "foo";

  @Test
  public void testMergesWithDefaults() {
    final String DATASET_URN = "foo";

    GlobalMetadata defaultMetadata = new GlobalMetadata();
    defaultMetadata.setDatasetUrn(DATASET_URN);

    GlobalMetadataCollector collector = new GlobalMetadataCollector(defaultMetadata, -1);
    GlobalMetadata metadataRecord = new GlobalMetadata();
    metadataRecord.setContentType(CONTENT_TYPE);

    GlobalMetadata mergedRecord = collector.processMetadata(metadataRecord);
    Assert.assertEquals(mergedRecord.getDatasetUrn(), DATASET_URN);
    Assert.assertEquals(mergedRecord.getContentType(), CONTENT_TYPE);
    Assert.assertEquals(1, collector.getMetadataRecords().size());
    Assert.assertTrue(collector.getMetadataRecords().contains(mergedRecord),
        "Expected merged record to exist in metadata cache");
  }

  @Test
  public void handlesNullRecord() {
    // If no defaults exist
    {
      GlobalMetadataCollector collectorWithNoDefaults = new GlobalMetadataCollector(-1);
      GlobalMetadata newRecord = collectorWithNoDefaults.processMetadata(null);
      Assert.assertNull(newRecord);
      Assert.assertEquals(collectorWithNoDefaults.getMetadataRecords().size(), 0);
    }

    // With defaults
    {
      GlobalMetadata defaults = buildMetadataWithUrn("DEFAULT");
      GlobalMetadataCollector collectorWithDefaults = new GlobalMetadataCollector(defaults, -1);
      GlobalMetadata newRecord = collectorWithDefaults.processMetadata(null);
      Assert.assertEquals(newRecord, defaults);
      Assert.assertEquals(collectorWithDefaults.getMetadataRecords().size(), 1);
    }
  }

  @Test
  public void handlesNullDefaults() {
      GlobalMetadataCollector collectorWithNoDefaults = new GlobalMetadataCollector(-1);
      GlobalMetadata record = buildMetadataWithContentType(CONTENT_TYPE);
      GlobalMetadata newRecord = collectorWithNoDefaults.processMetadata(record);

      Assert.assertEquals(newRecord, record);
      Assert.assertEquals(collectorWithNoDefaults.getMetadataRecords().size(), 1);
  }

  @Test
  public void testDoesNotStoreRecordTwice() {
      GlobalMetadata defaults = buildMetadataWithUrn("DEFAULT");
      GlobalMetadataCollector collectorWithDefaults = new GlobalMetadataCollector(defaults, -1);
      GlobalMetadata r1 = buildMetadataWithContentType(CONTENT_TYPE);
      GlobalMetadata newRecord = collectorWithDefaults.processMetadata(r1);
      Assert.assertNotNull(newRecord);

      GlobalMetadata r2 = buildMetadataWithContentType(CONTENT_TYPE);
      newRecord = collectorWithDefaults.processMetadata(r2);
      Assert.assertNull(newRecord);
      Assert.assertEquals(collectorWithDefaults.getMetadataRecords().size(), 1);
  }

  @Test
  public void evictsRecordsLRUBased() {
    GlobalMetadata r1 = buildMetadataWithContentType(CONTENT_TYPE + "_1");
    GlobalMetadata r1_1 = buildMetadataWithContentType(r1.getContentType());
    GlobalMetadata r2 = buildMetadataWithContentType(CONTENT_TYPE + "_2");
    GlobalMetadata r3 = buildMetadataWithContentType(CONTENT_TYPE + "_3");

    GlobalMetadataCollector collector = new GlobalMetadataCollector(2);
    GlobalMetadata newRecord;

    newRecord = collector.processMetadata(r1);
    Assert.assertNotNull(newRecord);

    newRecord = collector.processMetadata(r2);
    Assert.assertNotNull(newRecord);

    newRecord = collector.processMetadata(r1_1);
    Assert.assertNull(newRecord);

    // r2 should be evicted as r1 was more recently seen

    newRecord = collector.processMetadata(r3);
    Assert.assertNotNull(newRecord);

    Set<GlobalMetadata> cachedRecords = collector.getMetadataRecords();
    Assert.assertEquals(cachedRecords.size(), 2);
    Assert.assertTrue(cachedRecords.contains(r1));
    Assert.assertTrue(cachedRecords.contains(r3));
  }

  private GlobalMetadata buildMetadataWithUrn(String urn) {
    GlobalMetadata metadata = new GlobalMetadata();
    metadata.setDatasetUrn(urn);
    return metadata;
  }
  private GlobalMetadata buildMetadataWithContentType(String contentType) {
    GlobalMetadata metadata = new GlobalMetadata();
    metadata.setContentType(contentType);

    return metadata;
  }
}
