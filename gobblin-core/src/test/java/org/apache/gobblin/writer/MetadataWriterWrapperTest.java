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
package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.types.GlobalMetadata;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.type.RecordWithMetadata;


public class MetadataWriterWrapperTest {
  private MetadataWriterWrapper<byte[]> writer;
  private DummyWriter dummyWriter;
  private WorkUnitState state;

  @BeforeMethod
  public void setUp() {
    state = new WorkUnitState();
    dummyWriter = new DummyWriter();
    writer = new MetadataWriterWrapper<>(dummyWriter, byte[].class, 1, 0, state.getJobState());
  }

  @Test
  public void passesRecordThrough()
      throws IOException {
    byte[] record = new byte[]{'a', 'b', 'c', 'd'};
    dummyWriter.setExpectedRecord(record);

    writer.write(record);
    writer.commit();

    Assert.assertEquals(dummyWriter.recordsWritten(), 1);
    Assert.assertNull(state.getProp(ConfigurationKeys.WRITER_METADATA_KEY));
  }

  @Test
  public void recordsMetadata()
      throws IOException {
    final String URN = "unit-test:dataset";
    byte[] record = new byte[]{'a', 'b', 'c', 'd', 'e'};
    dummyWriter.setExpectedRecord(record);

    Metadata md = new Metadata();
    md.getGlobalMetadata().setDatasetUrn(URN);

    RecordWithMetadata<byte[]> mdRecord = new RecordWithMetadata<>(record, md);

    writer.write(mdRecord);
    writer.commit();

    Assert.assertEquals(dummyWriter.recordsWritten(), 1);

    String writerMetadata = state.getProp(ConfigurationKeys.WRITER_METADATA_KEY);
    Assert.assertNotNull(writerMetadata, "Expected there to be metadata");
    Assert.assertEquals(1, countOccurrences(writerMetadata, URN));

    // Write a 2nd record with the same metadata; it should _not_ be included twice in output
    byte[] record2 = new byte[]{'e', 'f', 'g', 'h'};
    dummyWriter.setExpectedRecord(record2);
    Metadata md2 = new Metadata();
    md2.getGlobalMetadata().setDatasetUrn(URN);

    writer.write(new RecordWithMetadata<>(record2, md2));
    writer.commit();
    Assert.assertEquals(dummyWriter.recordsWritten(), 2);

    writerMetadata = state.getProp(ConfigurationKeys.WRITER_METADATA_KEY);
    Assert.assertNotNull(writerMetadata, "Expected there to be metadata");
    Assert.assertEquals(1, countOccurrences(writerMetadata, URN));

    // and now a 3rd
    // Write a 2nd record with the same metadata; it should _not_ be included twice in output
    byte[] record3 = new byte[]{'i', 'j', 'k', 'l'};
    dummyWriter.setExpectedRecord(record3);
    Metadata md3 = new Metadata();
    md3.getGlobalMetadata().setDatasetUrn(URN + "_other");

    writer.write(new RecordWithMetadata<>(record3, md3));
    writer.commit();
    Assert.assertEquals(dummyWriter.recordsWritten(), 3);

    writerMetadata = state.getProp(ConfigurationKeys.WRITER_METADATA_KEY);
    Assert.assertNotNull(writerMetadata, "Expected there to be metadata");
    Assert.assertEquals(2, countOccurrences(writerMetadata, URN));
  }

  @Test
  public void testAppendsDefaultMetadata()
      throws IOException {
    state = new WorkUnitState();
    dummyWriter = new MetadataDummyWriter();
    writer = new MetadataWriterWrapper<>(dummyWriter, byte[].class, 1, 0, state.getJobState());

    byte[] recordBytes = new byte[]{'a', 'b', 'c', 'd'};
    Metadata md = new Metadata();
    md.getGlobalMetadata().addTransferEncoding("first");

    writer.write(new RecordWithMetadata<>(recordBytes, md));
    writer.commit();

    String writerMetadata = state.getProp(ConfigurationKeys.WRITER_METADATA_KEY);
    Assert.assertNotNull(writerMetadata, "Expected there to be metadata");

    int firstOccurrence = writerMetadata.indexOf("\"first\"");
    Assert.assertNotEquals(firstOccurrence, -1, "Expected to find record-level encoding in metadata");

    int secondOccurrence = writerMetadata.indexOf("\"default-encoding\"");
    Assert.assertNotEquals(secondOccurrence, -1, "Expected to find default metadata in metadata");

    Assert.assertTrue(firstOccurrence < secondOccurrence,
        "Expected recordBytes encoding to be present before default encoding");
  }

  @Test
  public void testAppendsMetadataWithNormalRecord() throws IOException {
    state = new WorkUnitState();
    dummyWriter = new MetadataDummyWriter();
    writer = new MetadataWriterWrapper<>(dummyWriter, byte[].class, 1, 0, state.getJobState());

    byte[] recordBytes = new byte[]{'a', 'b', 'c', 'd'};

    writer.write(recordBytes);
    writer.commit();

    String writerMetadata = state.getProp(ConfigurationKeys.WRITER_METADATA_KEY);
    Assert.assertNotNull(writerMetadata, "Expected there to be metadata");

    Assert.assertNotEquals(writerMetadata.indexOf("\"default-encoding\""),
        -1, "Expected to find default metadata in metadata");
  }

  private static int countOccurrences(String s, String stringToFind) {
    int start = s.indexOf(stringToFind, 0);
    int count = 0;

    while (start != -1) {
      count++;
      start = s.indexOf(stringToFind, start + stringToFind.length());
    }

    return count;
  }

  private static class DummyWriter implements DataWriter<byte[]> {
    private int rawRecordSeen;
    private byte[] expectedRecord;

    DummyWriter() {
      rawRecordSeen = 0;
    }

    public void setExpectedRecord(byte[] record) {
      this.expectedRecord = record;
    }

    @Override
    public void write(byte[] record)
        throws IOException {
      rawRecordSeen++;
      if (expectedRecord != null && !Arrays.equals(expectedRecord, record)) {
        throw new IOException("Expected record doesn't match");
      }
    }

    @Override
    public void commit()
        throws IOException {

    }

    @Override
    public void cleanup()
        throws IOException {

    }

    @Override
    public long recordsWritten() {
      return rawRecordSeen;
    }

    @Override
    public long bytesWritten()
        throws IOException {
      return 0;
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  private static class MetadataDummyWriter extends DummyWriter implements MetadataAwareWriter {
    private static GlobalMetadata globalMd;

    static {
      globalMd = new GlobalMetadata();
      globalMd.addTransferEncoding("default-encoding");
    }

    @Override
    public GlobalMetadata getDefaultMetadata() {
      return globalMd;
    }
  }
}
