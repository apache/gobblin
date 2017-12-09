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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;


public class MetadataConverterWrapperTest {
  @Test
  public void testConvertsMetadataNoOutput()
      throws DataConversionException {
    DummyConverter dummyConverter = new DummyConverter(0);
    MetadataConverterWrapper<String, String, String, String> wrapper = new MetadataConverterWrapper<>(dummyConverter);
    Iterable<RecordWithMetadata<String>> records =
        wrapper.convertRecord("foo", new RecordWithMetadata<String>("bar", buildMetadata(1)), new WorkUnitState());

    Assert.assertFalse(records.iterator().hasNext());
  }

  @Test
  public void testConvertsMetadataMultiOutput()
      throws DataConversionException {
    final int numRecordsToReturn = 2;
    DummyConverter dummyConverter = new DummyConverter(numRecordsToReturn);
    MetadataConverterWrapper<String, String, String, String> wrapper = new MetadataConverterWrapper<>(dummyConverter);

    Iterable<RecordWithMetadata<String>> records1 =
        wrapper.convertRecord("foo", new RecordWithMetadata<String>("bar", buildMetadata(1)), new WorkUnitState());
    Iterable<RecordWithMetadata<String>> records2 =
        wrapper.convertRecord("foo", new RecordWithMetadata<String>("baz", buildMetadata(2)), new WorkUnitState());

    Iterator<RecordWithMetadata<String>> record1It = records1.iterator();
    Iterator<RecordWithMetadata<String>> record2It = records2.iterator();

    for (int i = 0; i < numRecordsToReturn; i++) {
      RecordWithMetadata<String> record1 = record1It.next();
      Assert.assertEquals(record1.getRecord(), "converted" + String.valueOf(i));
      Assert.assertEquals(record1.getMetadata().getGlobalMetadata().getDatasetUrn(), "dataset-id:1");

      RecordWithMetadata<String> record2 = record2It.next();
      Assert.assertEquals(record2.getRecord(), "converted" + String.valueOf(i));
      Assert.assertEquals(record2.getMetadata().getGlobalMetadata().getDatasetUrn(), "dataset-id:2");
    }
  }

  @Test
  public void testAcceptsRawRecords() throws DataConversionException {
    final int numRecordsToReturn = 1;
    DummyConverter dummyConverter = new DummyConverter(numRecordsToReturn);
    MetadataConverterWrapper<String, String, String, String> wrapper = new MetadataConverterWrapper<>(dummyConverter);

    Iterable<RecordWithMetadata<String>> records =
        wrapper.convertRecord("foo", "bar", new WorkUnitState());
    Iterator<RecordWithMetadata<String>> recordsIt = records.iterator();

    RecordWithMetadata<String> record = recordsIt.next();
    Assert.assertFalse(recordsIt.hasNext());
    Assert.assertEquals(record.getRecord(), "converted0");
    Assert.assertEquals(record.getMetadata().getGlobalMetadata().getId(), "0");
  }

  private Metadata buildMetadata(int id) {
    Metadata md = new Metadata();
    md.getGlobalMetadata().setDatasetUrn("dataset-id:" + String.valueOf(id));
    return md;
  }

  private static class DummyConverter extends Converter<String, String, String, String> {
    private final int numRecordsToReturn;

    DummyConverter(int numRecordsToReturn) {
      this.numRecordsToReturn = numRecordsToReturn;
    }

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit)
        throws SchemaConversionException {
      return "";
    }

    @Override
    public Iterable<String> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      List<String> records = new ArrayList<>();
      for (int i = 0; i < numRecordsToReturn; i++) {
        records.add(String.format("converted%d", i));
      }

      return records;
    }
  }
}
