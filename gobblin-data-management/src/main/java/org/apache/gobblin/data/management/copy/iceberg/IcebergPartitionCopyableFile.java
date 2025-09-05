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
import org.apache.iceberg.util.SerializationUtil;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.data.management.copy.CopyableFile;


/**
 * An extension of {@link CopyableFile} that includes a base64-encoded Iceberg {@link DataFile}.
 */
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class IcebergPartitionCopyableFile extends CopyableFile {

  /**
   * Base64-encoded Iceberg {@link DataFile} associated with this copyable file.
   */
  private String base64EncodedDataFile;

  public IcebergPartitionCopyableFile(CopyableFile copyableFile, DataFile dataFile) {
    super(copyableFile.getOrigin(), copyableFile.getDestination(), copyableFile.getDestinationOwnerAndPermission(),
        copyableFile.getAncestorsOwnerAndPermission(), copyableFile.getChecksum(), copyableFile.getPreserve(),
        copyableFile.getFileSet(), copyableFile.getOriginTimestamp(), copyableFile.getUpstreamTimestamp(),
        copyableFile.getAdditionalMetadata(), copyableFile.datasetOutputPath, copyableFile.getDataFileVersionStrategy());
    this.base64EncodedDataFile = SerializationUtil.serializeToBase64(dataFile);
  }

  public DataFile getDataFile() {
    return SerializationUtil.deserializeFromBase64(base64EncodedDataFile);
  }
}
