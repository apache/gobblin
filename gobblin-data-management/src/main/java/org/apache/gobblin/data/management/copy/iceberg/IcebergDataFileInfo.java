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

package org.apache.gobblin.data.management.copy.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

import lombok.Builder;
import lombok.Data;


/**
 * Contains all the info related to a single data file.
 */
@Builder(toBuilder = true)
@Data
public class IcebergDataFileInfo {
  private final String srcFilePath;
  private String destFilePath;
  private FileFormat fileFormat;
  private long recordCount;
  private long fileSize;
  private StructLike partitionData;

  public DataFile getDataFile(PartitionSpec spec) {
    return DataFiles.builder(spec)
        .withPath(this.destFilePath)
        .withFormat(this.fileFormat)
        .withPartition(this.partitionData)
        .withRecordCount(this.recordCount)
        .withFileSizeInBytes(this.fileSize)
        .build();
  }
}