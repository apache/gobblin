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
import java.util.Set;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metadata.GlobalMetadataCollector;
import org.apache.gobblin.metadata.types.GlobalMetadata;
import org.apache.gobblin.type.RecordWithMetadata;
import org.apache.gobblin.util.ForkOperatorUtils;


/**
 * Wraps an existing {@link DataWriter} and makes it metadata aware. This class is responsible for doing the following:
 *
 * 1. If the underlying writer is {@link MetadataAwareWriter}, query it to get default metadata to add to any
 *    incoming records.
 * 2. Process each incoming record:
 *     2a. If it is a {@link RecordWithMetadata}, process the metadata and pass either the underlying record or the
 *         {@link RecordWithMetadata}, depending on the writer class type, to the wrapped writer.
 *     2b. If it is a standard record type attach any default metadata and pass the record to the wrapped writer.
 * 3. On {@link #commit()}, publish the combined metadata output to JobState property so it can be published.
 */
public class MetadataWriterWrapper<D> implements DataWriter<Object> {
  private final DataWriter wrappedWriter;
  private final Class<? extends D> writerDataClass;
  private final int numBranches;
  private final int branchId;
  private final State properties;
  private final GlobalMetadataCollector metadataCollector;

  /**
   * Initialize a new metadata wrapper.
   * @param wrappedWriter Writer to wrap
   * @param writerDataClass Class of data the writer accepts
   * @param numBranches # of branches in state
   * @param branchId Branch this writer is wrapping
   * @param writerProperties Configuration properties
   */
  public MetadataWriterWrapper(DataWriter<D> wrappedWriter, Class<? extends D> writerDataClass,
      int numBranches, int branchId, State writerProperties) {
    this.wrappedWriter = wrappedWriter;
    this.writerDataClass = writerDataClass;
    this.numBranches = numBranches;
    this.branchId = branchId;
    this.properties = writerProperties;

    GlobalMetadata defaultMetadata = null;
    if (wrappedWriter instanceof MetadataAwareWriter) {
      defaultMetadata = ((MetadataAwareWriter) wrappedWriter).getDefaultMetadata();
    }

    this.metadataCollector = new GlobalMetadataCollector(defaultMetadata, GlobalMetadataCollector.UNLIMITED_SIZE);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Object untypedRecord)
      throws IOException {
    if (untypedRecord instanceof RecordWithMetadata) {
      RecordWithMetadata record = (RecordWithMetadata)untypedRecord;

      GlobalMetadata globalMetadata = record.getMetadata().getGlobalMetadata();
      metadataCollector.processMetadata(globalMetadata);

      if (RecordWithMetadata.class.isAssignableFrom(writerDataClass)) {
        wrappedWriter.write(record);
      } else {
        wrappedWriter.write(record.getRecord());
      }
    } else {
      metadataCollector.processMetadata(null);
      wrappedWriter.write(untypedRecord);
    }
  }

  /**
   * Write combined metadata to the {@link ConfigurationKeys#WRITER_METADATA_KEY} parameter.
   */
  protected void writeMetadata() throws IOException {
    Set<GlobalMetadata> collectedMetadata = metadataCollector.getMetadataRecords();

    if (collectedMetadata.isEmpty()) {
      return;
    }

    String propName =
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_METADATA_KEY, numBranches, branchId);
    String metadataStr;
    if (collectedMetadata.size() == 1) {
      metadataStr = collectedMetadata.iterator().next().toJson();
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append('[');
      boolean first = true;

      for (GlobalMetadata md : collectedMetadata) {
        if (!first) {
          sb.append(',');
        }
        sb.append(md.toJson());
        first = false;
      }
      sb.append(']');
      metadataStr = sb.toString();
    }

    this.properties.setProp(propName, metadataStr);
  }

  @Override
  public void commit()
      throws IOException {
    writeMetadata();
    wrappedWriter.commit();
  }

  @Override
  public void cleanup()
      throws IOException {
    wrappedWriter.cleanup();
  }

  @Override
  public long recordsWritten() {
    return wrappedWriter.recordsWritten();
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return wrappedWriter.recordsWritten();
  }

  @Override
  public void close()
      throws IOException {
    wrappedWriter.close();
  }

  @Override
  public void flush() throws IOException {
    wrappedWriter.flush();
  }
}
