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
package org.apache.gobblin.metadata.types;

import java.io.IOException;

import org.apache.gobblin.metadata.MetadataMerger;
import org.apache.gobblin.writer.FsWriterMetrics;


/**
 * Merges a set of GlobalMetadata objects that have been serialized as JSON together to
 * create a final output.
 */
public class GlobalMetadataJsonMerger implements MetadataMerger<String> {
  private GlobalMetadata mergedMetadata;

  public GlobalMetadataJsonMerger() {
    mergedMetadata = new GlobalMetadata();
  }

  @Override
  public void update(String metadata) {
    try {
      GlobalMetadata parsedMetadata = GlobalMetadata.fromJson(metadata);
      mergedMetadata.addAll(parsedMetadata);
    } catch (IOException e) {
      throw new IllegalArgumentException("Error parsing metadata", e);
    }
  }

  @Override
  public void update(FsWriterMetrics metrics) {
    long numRecords = mergedMetadata.getNumRecords();
    int numFiles = mergedMetadata.getNumFiles();

    for (FsWriterMetrics.FileInfo fileInfo: metrics.getFileInfos()) {
      numRecords += fileInfo.getNumRecords();
      numFiles += 1;

      mergedMetadata.setFileMetadata(fileInfo.getFileName(), GlobalMetadata.NUM_RECORDS_KEY,
          Long.valueOf(fileInfo.getNumRecords()));
    }

    mergedMetadata.setNumRecords(numRecords);
    mergedMetadata.setNumOutputFiles(numFiles);
  }

  @Override
  public String getMergedMetadata() {
    try {
      return mergedMetadata.toJson();
    } catch (IOException e) {
      throw new AssertionError("Unexpected IOException serializing to JSON", e);
    }
  }
}
