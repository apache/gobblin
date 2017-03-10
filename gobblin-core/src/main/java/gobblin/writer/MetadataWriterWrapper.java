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
package gobblin.writer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metadata.types.GlobalMetadata;
import gobblin.type.RecordWithMetadata;
import gobblin.util.ForkOperatorUtils;

// TODO JAVADOC
public class MetadataWriterWrapper<D> implements DataWriter<Object> {
  private final DataWriter wrappedWriter;
  private final GlobalMetadata defaultMetadata;
  private final Set<GlobalMetadata> collectedMetadata;
  private final Class<? extends D> writerClass;
  private final int numBranches;
  private final int branchId;
  private final State properties;

  // Optimization - cache the last seen GlobalMetadata so we don't bother
  // merging records if they are unchanged
  private String lastSeenMetadataId;

  public MetadataWriterWrapper(DataWriter<D> wrappedWriter, Class<? extends D> writerClass,
      int numBranches, int branchId, State writerProperties) {
    this.wrappedWriter = wrappedWriter;
    this.writerClass = writerClass;
    this.collectedMetadata = new HashSet<>();
    this.lastSeenMetadataId = "";
    this.numBranches = numBranches;
    this.branchId = branchId;
    this.properties = writerProperties;

    if (wrappedWriter instanceof MetadataAwareWriter) {
      this.defaultMetadata = ((MetadataAwareWriter) wrappedWriter).getDefaultMetadata();
    } else {
      this.defaultMetadata = new GlobalMetadata();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Object untypedRecord)
      throws IOException {
    if (untypedRecord instanceof RecordWithMetadata) {
      RecordWithMetadata record = (RecordWithMetadata)untypedRecord;

      GlobalMetadata globalMetadata = record.getMetadata().getGlobalMetadata();
      if (!lastSeenMetadataId.equals(globalMetadata.getId())) {
        // TODO should there be a callback on newMetadata?
        lastSeenMetadataId = globalMetadata.getId();
        globalMetadata.mergeWithDefaults(defaultMetadata);
        collectedMetadata.add(globalMetadata);
      }

      if (RecordWithMetadata.class.isAssignableFrom(writerClass)) {
        wrappedWriter.write(record);
      } else {
        wrappedWriter.write(record.getRecord());
      }
    } else {
      if (!defaultMetadata.isEmpty()) {
        collectedMetadata.add(defaultMetadata);
      }
      wrappedWriter.write(untypedRecord);
    }
  }

  protected void writeMetadata() throws IOException {
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
}
